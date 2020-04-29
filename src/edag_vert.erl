%%%-------------------------------------------------------------------
%%% @author zxh <robertzhouxh@gmail.com>
%%% @copyright (C) 2020, zxh
%%% @doc
%%%
%%% @end
%%% Created : 14 Mar 2020 by zxh <robertzhouxh@gmail.com>
%%%-------------------------------------------------------------------
-module(edag_vert).

-author("Xuehao Zhou<robertzhouxh@gmail.com>").

-include("edag.hrl").

-behaviour(gen_server).

-export([start_link/3]).
-export([start_node/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([from_vdef/1, new/0, idle/1, done/1, cancel/1, fail/2, is_idle/1, is_done/1, to_list/1]).
-export([name/1,deps/1,desc/1,type/1,indata/1, outdata/1, trans/1, reason/1, kv/1, mod/1, timeout/1, status/1]).
-export([set_name/2, set_type/2, set_module/1, set_desc/2, set_trans/2, set_deps/2]).
-export([set_kv/2, set_status/2, set_timeout/2]).

-type name_t()    :: any().
-type deps_t()    :: list().
-type module_t()  :: module() | undefined.
-type kv_t()      :: map().
-type timeout_t() :: pos_integer() | undefined.
-type setter_t()  :: fun((t(), any()) -> t()).

-record(state, {
		vid                :: term(),
		component          :: module_t(),
		vert               :: term(),
		trans = #{}   :: kv_t(),
		parent             :: pid(),
		cb_state           :: term(),
		flow_mode = pull   :: push | pull,
		fail_mode = global :: partial | global,
		outports           :: list(#subscription{}),
		inports            :: list(),
		emitted = 0        :: non_neg_integer()
	       }).

-define(T, ?MODULE).
-record(?T, {
	     %% mandatory
	     name       = undefined  :: name_t(),
	     deps       = []         :: deps_t(),

	     %% optional
	     desc      = <<>>        :: term(),
	     type      = entity      :: name_t(),
	     indata    = #{}         :: kv_t(),
	     outdata   = #{}         :: kv_t(),
	     trans     = #{}         :: kv_t(),
	     status    = idle        :: idle | running | done | failed | cancelled | undefined,
	     module    = edag_cbmodx :: module_t(),
	     kv        = #{}         :: kv_t(),
	     reason    = undefined   :: term(),
	     timeout   = undefined   :: timeout_t()
	    }).

-type t() :: #?T{}.
-export_type([t/0]).

%%%-------------------------------------------------------------------
%%% API

start_link(VId, Vert, Parent) ->
    gen_server:start_link({local, VId}, ?MODULE, [name(Vert), Vert, Parent], []).
%% gen_server:start_link({local, ?MODULE}, ?MODULE, [VId, Vert, Parent], []).
%% gen_server:start_link(?MODULE, [VId, Vert, Parent], []).

start_node(Server, InPorts, OutPorts, FlowMode) -> 
    gen_server:call(Server, {start, InPorts, OutPorts, FlowMode}).

%%%-------------------------------------------------------------------
%%% gen_server callbacks
init([VId, #?T{module = Mod} = Vert, Parent]) ->
    {module, Mod} = code:ensure_loaded(Mod),
    {ok, #state{vid = VId, component= Mod, vert = Vert, parent = Parent}}.

handle_call({start, InPorts, OutPorts, FlowMode}, _From, State=#state{component= Mod, vert=Vert}) ->
    {ok, NewCbState} = Mod:init(Vert),
    {reply, ok, State#state{flow_mode = FlowMode, 
			    outports = OutPorts, 
			    inports =  InPorts,
			    cb_state = NewCbState}};

%% receive sycn pull request, and publish the item value to the sender vertex
handle_call({request_pull, ReqVId}, _From, State=#state{outports = Subs}) ->
    Sub = lists:keyfind(ReqVId,2,Subs),
    {Reply, NSub} = handle_sync_request(Sub),
    NSubs = lists:keystore(ReqVId,2,Subs, NSub),
    {reply, Reply, State#state{outports=NSubs}};
handle_call(_What, _From, State) -> 
    {reply, State}.

handle_cast(_Request, State) -> 
    {noreply, State}.

%% receive the item value from the inport vertices, triggered by sync or async request_pull
handle_info({item, Value}, State = 
		#state{vid=VId,cb_state=CbState, 
		       component=Mod, outports=Subs, 
		       flow_mode=_FMode, parent=Parent}) ->
    NewState =
	case Mod:process(Value, CbState) of
	    {emit, Data, NCbState} ->
		NewSubs = out_vert(Subs, Data),
		%% tell the DAG manager, i am done
		Parent ! {trigger, VId},
		State#state{outports = NewSubs, cb_state = NCbState};
	    {request, PPids, NCbState} when is_list(PPids) ->
		%% tell the current vert, you should send pull request to all the PPids
		maybe_request_items(PPids, _FMode),
		State#state{cb_state = NCbState};
	    {emit_request, Emitted, PPids, NCbState} when is_list(PPids) ->
		NewSubs = out_vert(Subs, Emitted),
		maybe_request_items(PPids, _FMode),
		State#state{outports = NewSubs, cb_state = NCbState};
	    {ok, NCbState} ->
		State#state{cb_state = NCbState};
	    {error, _What} ->
		exit(_What)
	end,
    {noreply, NewState};

%% send sync pull request and collect all the response from the publisher vertices
handle_info({nextverts, syn}, 
	    State=#state{vid=VId,
			 cb_state=CbState, 
			 inports=InPorts, 
			 component=Mod, 
			 outports=Subs, 
			 parent=Parent}) ->
    NAcc = lists:foldl(
	    fun({PublisherId, VPid}, Acc) -> 
		    ValFromPublisher = gen_server:call(VPid, {request_pull, VId}),
		    [{PublisherId, ValFromPublisher}|Acc]
	    end, [], InPorts), 
    NState = 
	case Mod:process(NAcc, CbState) of
	    {emit, Value, NCbState} ->
		NewSubs = out_vert(Subs, Value),
		Parent ! {trigger, VId},
		State#state{outports = NewSubs, cb_state = NCbState};
	    {ok, NCbState} ->
		State#state{cb_state = NCbState};
	    {error, _What} ->
		exit(_What)
	end,
    {noreply, NState};

%% send async pull request and the publisher will push the item to the sender vertex
handle_info({nextverts, asyn}, State = #state{vid = NId, inports = InPorts}) ->
    lists:foreach(
      fun({_VId, VPid}) ->
	      VPid ! {request_pull, self(), NId} 
      end, InPorts),
    {noreply, State};

%% receive asycn pull request, and publish the item value to the sender vertex
handle_info({request_pull, ReqVPid, _ReqVId}, State = #state{outports = Subs}) ->
    NewSubs = 
	lists:map(
	  fun(Sub) -> 
		  handle_async_request(Sub,ReqVPid)
	  end, Subs),
    {noreply, State#state{outports =  NewSubs}};

%% push-mode 
handle_info({emit, Value}, State = #state{outports = Subs, flow_mode = FMode, emitted = EmitCount}) ->
    NewSubs = out_vert(Subs, Value),
    NewState = State#state{outports = NewSubs},
    request_all(State#state.inports, FMode),
    {noreply, NewState#state{emitted = EmitCount+1}};

handle_info(stop, State=#state{component = Mod, cb_state = CBState}) ->
    case erlang:function_exported(Mod, shutdown, 1) of
	true -> Mod:shutdown(CBState);
	false -> ok
    end,
    {stop, normal, State};

handle_info(_Req, State) -> {noreply, State}.

terminate(_Reason, _State) -> 
    ok.
code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

%%%--------------------------------------------------------------------------------
%%% Internal functions

handle_sync_request(Sub = #subscription{flow_mode = pull, out_buffer = Buffer}) -> 
    case queue:out(Buffer) of
	{{value, Value}, Q2} -> 
	    {Value, Sub#subscription{out_buffer = Q2, pending = false}};
	{empty, _Q1} -> 
	    {undefined, Sub#subscription{pending = true}}
    end;
handle_sync_request(Sub = #subscription{flow_mode = push}) -> {undefined, Sub}.

handle_async_request(Sub = #subscription{flow_mode = pull,subpid = SubPid,out_buffer=Buffer},SubPid) ->
    case queue:out(Buffer) of
	{{value, Value}, Q2} -> 
	    SubPid ! {item, Value},
	    Sub#subscription{out_buffer = Q2, pending = false};
	{empty, _Q1} -> 
	    Sub#subscription{pending = true}
    end;
handle_async_request(Sub = #subscription{flow_mode = pull}, _SubPid) -> Sub;
handle_async_request(Sub = #subscription{flow_mode = push}, _SubPid) -> Sub.

out_vert(Subs, Value) when is_map(Subs) ->
    maps:fold(
      fun({VId, Sub}, Acc) -> 
	      Data = proplists:get_value(VId, Value),
	      NSub = vert_emit(Sub, Data),
	      maps:put(VId, NSub, Acc)
      end, #{}, Subs);
out_vert(Subs, Value) when is_list(Subs) ->
    NewSubs = 
	lists:map(
	  fun(Sub) -> 
		  Data = proplists:get_value(Sub#subscription.subid, Value),
		  vert_emit(Sub, Data) 
	  end, Subs),
    NewSubs.

vert_emit(Sub = #subscription{flow_mode = push, subpid = SubPid}, Input) ->
    SubPid ! {item, Input},
    Sub;
vert_emit(Sub = #subscription{flow_mode=pull, pending=false, out_buffer=Buffer,subid=_SubId}, Input) ->
    Sub#subscription{out_buffer = queue:in(Input, Buffer)};
vert_emit(Sub = #subscription{flow_mode = pull, pending = true, subpid = SubPid}, Input) ->
    SubPid ! {item, Input},
    Sub#subscription{pending = false}.

%% ----------------------------------------------------------------------------
%% TODO:

request_all(_Inports, push) -> ok;
request_all(Inports, pull) ->
    %% {_VIds, InPids} = lists:unzip(Inports),
    maybe_request_items(Inports, pull).
maybe_request_items(_Inports, push) -> ok;
maybe_request_items(Inports, pull) -> request_items(Inports).
request_items(Inports) when is_list(Inports) -> 
    [VPid ! {request, self()} || {_VId, VPid} <- Inports].

%% ------------------------ for vertex build ------------------------------------
new() -> #?T{}.

set_name(     T = #?T{},    Name)        -> T#?T{name       = Name}.
set_deps(     T = #?T{},    Deps)        -> T#?T{deps       = Deps}.
set_type(     T = #?T{},    Type)        -> T#?T{type       = Type}.
set_indata(   T = #?T{},    InData)      -> T#?T{indata     = InData}.
set_trans(    T = #?T{},    Trans)       -> T#?T{trans      = Trans}.
set_outdata(  T = #?T{},    OutData)     -> T#?T{outdata    = OutData}.
set_desc(     T = #?T{},    Desc)        -> T#?T{desc       = Desc}.
set_kv(       T = #?T{},    Kv)          -> T#?T{kv         = Kv}.
set_timeout(  T = #?T{},    Timeout)     -> T#?T{timeout    = Timeout}.
set_reason(   T = #?T{},    Reason)      -> T#?T{reason     = Reason}.
set_status(   T = #?T{},    Status)      -> T#?T{status     = Status}.

set_module(T = #?T{type = <<"entity">>}) -> T#?T{module     = dccore_dag_entity};
set_module(T = #?T{type = <<"joiner">>}) -> T#?T{module     = dccore_dag_joiner};
set_module(T = #?T{type = <<"copier">>}) -> T#?T{module     = undefined};
set_module(T = #?T{type = <<"spliter">>})-> T#?T{module     = undefined};
set_module(T = #?T{type = <<"filter">>}) -> T#?T{module     = undefined};
set_module(T = #?T{type = _Unknown})     -> T#?T{module     = undefined}.

name(#?T{name            = Name})      -> Name.
deps(#?T{deps            = Deps})      -> Deps.
type(#?T{type            = Type})      -> Type.
indata(#?T{indata        = InData})    -> InData.
outdata(#?T{outdata      = OutData})   -> OutData.
desc(#?T{desc            = Desc})      -> Desc.
trans(#?T{trans          = Trans})     -> Trans.
mod(#?T{module           = Module})    -> Module.
kv(#?T{kv                = Kv})        -> Kv.
timeout(#?T{timeout      = Timeout})   -> Timeout.
reason(#?T{reason        = Reason})    -> Reason.
status(#?T{status        = Status})    -> Status.

from_vdef(VDef) when is_map(VDef) ->
    Res0 = new(),
    MandatoryFields = [
		       {<<"name">>,    fun set_name/2},
		       {<<"deps">>,    fun set_deps/2}
		      ],
    OptionalFields = [
		      {<<"type">>,     fun set_type/2},
		      {<<"indata">>,   fun set_indata/2},
		      {<<"outdata">>,  fun set_outdata/2},
		      {<<"desc">>,     fun set_desc/2},
		      {<<"trans">>,    fun set_trans/2},
		      {<<"kv">>,       fun set_kv/2},
		      {<<"status">>,   fun set_status/2},
		      {<<"timeout">>,  fun set_timeout/2}
		     ],
    case load_mandatory(Res0, VDef, MandatoryFields) of
	{error, _} = Err -> Err;
	{ok, {Res1, Map1}} ->
	    {ok, {Res2, Map2}} = load_optional(Res1, Map1, OptionalFields),
	    Res3 = set_kv(Res2, Map2),
	    {ok, Res3}
    end;
from_vdef(T = #?T{}) -> {ok, T};
from_vdef(_) -> {error, not_map}.

is_idle(Vert = #?T{}) ->
    case status(Vert) of
	idle ->
	    true;
	_ ->
	    false
    end.
is_done(Vert = #?T{}) ->
    case status(Vert) of
	done ->
	    true;
	_ ->
	    false
    end.

idle(Vert = #?T{})         -> set_status(Vert, idle).
cancel(Vert = #?T{})       -> set_status(Vert, cancelled).
done(Vert = #?T{})         -> set_status(Vert, done).
fail(Vert = #?T{}, Reason) ->
    Vert2 = set_reason(Vert, Reason),
    set_status(Vert2, failed).

-spec to_list(t()) -> [{atom(), any()}].
to_list(T = #?T{}) ->
    Fields = record_info(fields, ?T),
    [_Tag | Values] = tuple_to_list(T),
    lists:zip(Fields, Values).

-spec load_mandatory(t(), map(), [{atom(), setter_t()}]) -> {ok, {t(), map()}} | {error, any()}.
load_mandatory(Vert, VDef, KeySetterPairs) -> load(Vert, VDef, KeySetterPairs, error).
load_optional(Vert, VDef, KeySetterPairs)  -> load(Vert, VDef, KeySetterPairs, continue).

-spec load(t(), map(), [{atom(), setter_t()}], error | continue) -> {ok, {t(), map()}} | {error, any()}.
load(Vert = #?T{}, VDef, [], _FailMode) -> {ok, {Vert, VDef}};
load(Vert = #?T{}, VDef, [{Key, WithKeySetter} | Rest], FailMode) ->
    case {VDef, FailMode} of
	{#{Key := Val}, _FailMode} ->
	    Vert2 = WithKeySetter(Vert, Val),
	    Map2 = maps:remove(Key, VDef),
	    load(Vert2, Map2, Rest, FailMode);
	{_, error} ->
	    {error, {missing_key, Key}};
	{_, continue} ->
	    load(Vert, VDef, Rest, FailMode)
    end.
