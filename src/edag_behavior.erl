-module(edag_behavior).

-author("Xuehao Zhou<robertzhouxh@gmail.com>").

-callback init(CbState :: term()) -> {ok, term()}.
-callback process(Value :: term(), State :: term())
       -> {ok, term()} | 
	  {emit, {Value :: term()}, term()} | 
	  {request, {ReqPid :: pid() }, term} |
	  {emit_request, {Value :: term()}, {ReqPid :: pid()}, term()} | 
	  {error, Reason :: term()}.
-callback shutdown(State :: term()) -> any().


