%%%-------------------------------------------------------------------
%%% @author zxh <robertzhouxh@gmail.com>
%%% @copyright (C) 2020, zxh
%%% @doc
%%%
%%% @end
%%% Created : 14 Mar 2020 by zxh <robertzhouxh@gmail.com>
%%%-------------------------------------------------------------------
-module(edag).

-author("Xuehao Zhou<robertzhouxh@gmail.com>").

-behaviour(gen_server).

-include("edag.hrl").

%% API
-export([start_link/2]).
-export([run_task/2]).
-export([create_graph/2]).
-export([start_graph/1, start_graph/3, stop_graph/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).
-define(id(), erlang:unique_integer([positive, monotonic])).

-record(state, {
		gid            :: non_neg_integer() | string(),
		graph,
		gdef,
		status         :: idle | started | running,
		waitverts = [] :: list()
	       }).

-type name_t() :: any().
-type dag_t() :: digraph:graph().
%% -type edge_t() :: digraph:edge().
-type label_t() :: digraph:label().
-type vert_t() :: digraph:vertex().

-define(T, ?MODULE).
-record(?T, {
    children = #{}   :: #{vert_t() => pid()},
    dag      = new() :: dag_t()
}).

-type t() :: #?T{}.
-export_type([t/0]).

%%%===================================================================
%%% API

create_graph(GId, GDef) when is_map(GDef) -> edag_sup:new(GId, GDef).

start_graph(GId) -> gen_server:call(GId, {start, pull, global}).
start_graph(GId, FlowMode, FailMode) -> gen_server:call(GId, {start, FlowMode, FailMode}).

stop_graph(GId) -> GId ! stop.

run_task(GId, Task) -> gen_server:call(GId, {task, Task}).

start_link(GId, GDef) -> 
    gen_server:start_link({local, to_atom(GId)}, ?MODULE, [GId, GDef], []).
    %% gen_server:start_link({local, ?SERVER}, ?MODULE, [GId, GDef], []).
    %% gen_server:start_link(?MODULE, [GId, GDef], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([GId, #{<<"dag_orchestration">> := DAG} = GDef]) ->
    process_flag(trap_exit, true),
    {ok, G} = from_gdef(DAG),
    {ok, #state{gid = GId, gdef = GDef, graph = G, status = idle}}.

handle_call({start, FlowMode, _FailMode}, _From, State=#state{graph = T, gid = GId}) ->
    G1 = lists:foldl(
	   fun(VId, Acc) ->
		   {_VId, Vert} = vertex(T, VId),
		   VVId = to_binary(VId),
		   GGId = to_binary(GId),
		   NVId = to_atom(<<GGId/binary, "-", VVId/binary>>),
		   NAcc = 
		       case edag_vert:start_link(NVId, Vert, self()) of
			   {ok, VPid}  -> child_started(Acc, VId, VPid);
			   _Err -> Acc
		       end,
		   NAcc
	   end, T, vertices(T)),
    Neighbours = with_neighbours(G1, FlowMode),
    #?T{children = C} = G1,
    lists:foreach(
      fun({VId, VPid}) ->
	      {InPorts, OutPorts}= proplists:get_value(VId, Neighbours),
	      edag_vert:start_node(VPid, InPorts, OutPorts, FlowMode)
      end, maps:to_list(C)),
    case FlowMode of
	push -> ok;
	%% pull -> lists:foreach(fun({_VId, VPid}) -> VPid ! pull end, Nodes)
	pull -> ok
    end,
    {reply, ok, State#state{status = running, graph = G1}};

%% send task to the source vertices process
handle_call({task, [Task]}, _From, #state{graph = G} = State) ->
    Waits = trigger(G, {item, maps:to_list(Task)}),
    {reply, Waits, State#state{waitverts = Waits}};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({trigger, VId}, State = #state{graph = T, waitverts = WaitVerts}) -> 
    NewWaits = lists:delete(VId, WaitVerts),
    T1 = child_done(T, VId),
    Waits = 
	case NewWaits of
	    [] -> trigger(T1, {nextverts, syn});
	    _ -> NewWaits
	end,
    case Waits of
	[] -> 
	    reset_verts(T);
	_Else -> ok
    end,
    {noreply, State#state{graph = T1, waitverts = Waits}};
handle_info(stop, State=#state{status = Status, graph = #?T{children = C}}) ->
    case Status of
	%% running -> lists:foreach(fun(VPid) -> VPid ! stop end, maps:keys(C));
	running -> lists:foreach(fun(VPid) -> VPid ! stop end, maps:values(C));
	_ -> ok
    end,
    {stop, normal, State};

handle_info({'EXIT', _Pid, {shutdown, {ok, _Data}}}, State) -> start_verts_or_exit(State);
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
format_status(_Opt, Status) -> Status.

%%%---------------------------------------------------------------------
%%% Internal functions

to_binary(Val) when is_list(Val)    -> list_to_binary(Val);
to_binary(Val) when is_atom(Val)    -> atom_to_binary(Val, utf8);
to_binary(Val) when is_integer(Val) -> integer_to_binary(Val);
to_binary(Val)                      -> Val.

to_atom(Val) when is_atom(Val)   -> Val;
to_atom(Val) when is_list(Val)   -> list_to_atom(Val);
to_atom(Val) when is_binary(Val) -> binary_to_atom(Val, utf8).

new() -> digraph:new([acyclic, protected]).

-spec add_vertices(t(), [t()]) -> ok.
add_vertices(#?T{dag = Dag}, Verts) ->
    lists:foreach(
      fun(Vert) ->
	      Name = edag_vert:name(Vert),
	      digraph:add_vertex(Dag, Name, Vert)
      end, Verts).

-spec add_vertex(t(), vert_t(), t()) -> t().
%% add or update the Vertex
add_vertex(T = #?T{dag = Dag}, Name, Vert) ->
    digraph:add_vertex(Dag, Name, Vert),
    T.

add_dependencies(_T = #?T{}, []) -> ok;
add_dependencies(T = #?T{}, [Vert | Rest]) ->
    Name = edag_vert:name(Vert),
    Deps = edag_vert:deps(Vert),
    case add_edges(T, Name, Deps) of
        ok -> 
	    add_dependencies(T, Rest);
        {error, _} = Err -> Err
    end.

-spec add_edges(t(), name_t(), [name_t()]) -> ok | {error, any()}.
add_edges(_T, _To, []) -> ok;
add_edges(T = #?T{dag = Dag}, To, [From | Rest]) ->
    case digraph:add_edge(Dag, From, To) of
        {error, _} = Err -> 
	    Err;
        _ -> 
	    add_edges(T, To, Rest)
    end.

-spec vertex(t(), vert_t()) -> {vert_t(), label_t()} | false.
vertex(#?T{dag = Dag}, Vert) -> digraph:vertex(Dag, Vert).

-spec vertices(t()) -> [vert_t()].
vertices(#?T{dag = Dag}) -> digraph:vertices(Dag).

%% -spec edge(t(), vert_t()) -> {edge_t(), vert_t(), vert_t(), label_t()} | false.
%% edge(#?T{dag = Dag}, Edge) -> digraph:edge(Dag, Edge).
%% -spec edges(t()) -> [edge_t()].
%% edges(#?T{dag = Dag}) -> digraph:edges(Dag).

-spec is_deps_done(t(), vert_t()) -> boolean().
is_deps_done(T, Vert) ->
    lists:all(
      fun(Dep) ->
	      {_, DepV} = vertex(T, Dep),
	      edag_vert:is_done(DepV)
      end, edag_vert:deps(Vert)).

-spec ready_verts(t()) -> [vert_t()].
ready_verts(T = #?T{}) -> 
    lists:filter(
      fun(VId) -> 
	      {_, Vert} = vertex(T, VId),
	      edag_vert:is_idle(Vert) andalso is_deps_done(T, Vert)
      end, vertices(T)).

-spec reset_verts(t()) -> [vert_t()].
reset_verts(T = #?T{}) -> 
    lists:map(fun(VId) -> child_idle(T, VId) end, vertices(T)).

%% -spec format(t()) -> string().
%% format(T = #?T{}) ->
%%     Verts = [V || V <- vertices(T)],
%%     Edges = [E || E <- edges(T)],
%%     io_lib:format("~p~n", [{Verts, Edges}]).

from_gdef(GDefProps) ->
    case make_vertices(GDefProps) of
        {ok, Verts} ->
            T = #?T{},
            ok = add_vertices(T, Verts),
            case add_dependencies(T, Verts) of
                ok ->
                    {ok, T};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

make_vertices(GDefs) -> make_vertices2(GDefs, []).

make_vertices2([], Acc) -> {ok, Acc};
make_vertices2([VDef|Rest], Acc) ->
    case edag_vert:from_vdef(VDef) of
        {ok, Vert0} ->
	    %% TODO:
	    Vert = edag_vert:set_module(Vert0),
            make_vertices2(Rest, [Vert|Acc]);
        {error, _} = Err ->
            Err
    end.

with_neighbours(#?T{children = C} = G, FlowMode) ->
    lists:foldl(
      fun(VId, Acc) ->
	      InOut= build_neighbours(G, VId, FlowMode),
	      [{VId, InOut}|Acc]
      end, [], maps:keys(C)).

build_neighbours(#?T{dag = Dag, children = C}, VId, FlowMode) ->
    InNeighbours = digraph:in_neighbours(Dag, VId),
    OutNeighbours = digraph:out_neighbours(Dag, VId),
    InPorts = lists:foldl(
		fun(E, Acc) ->
			InPid = maps:get(E, C),
			[{E, InPid}|Acc]
		end, [], InNeighbours),
    OutPorts = lists:foldl(
		 fun(E, Acc) ->
			 SubPid = maps:get(E, C),
			 Sub = #subscription{
				  flow_mode  = FlowMode,
				  subid      = E,
				  subpid     = SubPid,
				  out_buffer = queue:new()
				 },
			 [Sub|Acc]
		 end, [], OutNeighbours),
    {InPorts, OutPorts}.
    
trigger(#?T{children = C} = G, Cmd) ->
    ReadyVerts = ready_verts(G),
    lists:foreach(
      fun(VId) ->
	      VPid = maps:get(VId, C),
	      VPid ! Cmd 
      end, ReadyVerts),
    ReadyVerts.

-spec child_started(t(), vert_t(), pid()) -> t().
child_started(T = #?T{children = Children}, Name, Pid) ->
    Children2 = Children#{Name => Pid},
    T#?T{children = Children2}.

is_finished(T) ->
    lists:all(fun(VertName) ->
        {VertName, Vert} = vertex(T, VertName),
        edag_vert:is_done(Vert)
    end, vertices(T)).

child_idle(T = #?T{}, VId) ->
    {Name, Vert} = vertex(T, VId),
    V1 = edag_vert:idle(Vert),
    add_vertex(T, Name, V1),
    T.
child_done(T = #?T{}, VId) ->
    {Name, Vert} = vertex(T, VId),
    V1 = edag_vert:done(Vert),
    add_vertex(T, Name, V1),
    T.

%% TODO:
start_verts_or_exit(State = #state{graph = G}) ->
    case is_finished(G) of
        true -> {stop, normal, State};
        false ->
	    %% %% next partial vertex start
            %% {ok, State2} = start_verts(State),
            {noreply, State}
    end.
