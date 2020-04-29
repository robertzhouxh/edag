edag
=====
    An Erlang DAG Orchestratin lighe Engine writen in erlang.
	
Design
------
 
 ```                                                   
                                                       | 1. receive the create cmd to create a edag instance with the input dag defination.
                                                       | 2. receive the start cmd to start all the vertices instance, each vertex act as gen_server
          |------------------------------------|       | 3. receive the task, and pick out the first batch vertices[vertex1] to run
     ---->| edag(gen_server) act as dag-manager| ------| 4. vertex-1 run the task, and send done signal to the edag mannager.
          |------------------------------------|       | 5. edag manager pick out the secend batch vertices, and send run signal to the vertices[vertex-2,3]
		                                               | 6. vertex-2,3 finished, and send the finish signal to edag mananger.
                                                       | 7. edag manager pick out the third batch vertices, and sned run signal to the vertices[vertex-5,6]
													   | 8. vertex-5,6 finished, and send the finish signal to the edag mangaer
													   | 9. edag manager pickout the final batch vertices, and send the run signal to the vertices[vertex7]
													   | 10. vertex-10 finished and send finish signal to edag manager, the edag reset all the vervices to the idle status.



                                       |----------------------|           |----------------------|
                                       | vertex-2(gen_server) |---------->| vertex-5(gen_server) |-------------\
                                      /|----------------------|           |----------------------|              \
									 /                                                                           \
                                    /                                                                             \
                                   /                                                                               \
                                  /                                                                                 \
          |----------------------/                                                                                   |----------------------|
          | vertex-1(gen_server) |                                                                                   | vertex-7(gen_server) |
          |----------------------\                                                                                   |----------------------|
		                          \                                                                                  /
                                   \                                                                                /
								    \                                                                              /
                                     \                                                                            /
                                      \                                                           |              /
                                       |----------------------|            |----------------------|             /
                                       | vertex-3(gen_server) |----------->| vertex-6(gen_server) |-------------
                                       |----------------------|            |----------------------|


```


Build
-----

    $ rebar3 compile
