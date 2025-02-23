#META:SID=[0-10]

SID = System.env.get("SID").toInteger(); 

def execute_query(g,id,i,ORDER_j,DATABASE,DATASET,QUERY,ITERATION,OBJECT_ARRAY,SID){

	t = System.nanoTime();
	v = g.v(id).id;
	exec_time = System.nanoTime() - t;

        //DATABASE,DATASET,QUERY,SID,ITERATION,ORDER,TIME,OUTPUT,PARAMETER1(NODE)
	result_row = [ DATABASE, DATASET, QUERY, String.valueOf(SID), ITERATION, String.valueOf(ORDER_j), String.valueOf(exec_time),v, String.valueOf(OBJECT_ARRAY[i])];
	println result_row.join(',');
}

if (SID == NODE_LID_ARRAY.size()) { 
	order_j = 1;
	for (i in RAND_ARRAY) {
	    execute_query(g,NODE_LID_ARRAY[i],i,order_j,DATABASE,DATASET,QUERY,ITERATION,NODE_ARRAY,SID);
	    order_j++;
	}
} else {
	 execute_query(g,NODE_LID_ARRAY[SID],SID,0,DATABASE,DATASET,QUERY,ITERATION,NODE_ARRAY,SID);
}

//g.shutdown();
