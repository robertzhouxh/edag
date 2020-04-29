-record(subscription,
	{
	 subid,
	 subpid,
	 out_buffer,
	 pending = false,
	 flow_mode = pull
	}).
