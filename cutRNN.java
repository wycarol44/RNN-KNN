  package com.hp.hpl.CHAOS.RNN.copy;

import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;

import com.hp.hpl.CHAOS.Aggregation.Configure;
import com.hp.hpl.CHAOS.Queue.StreamQueue;
import com.hp.hpl.CHAOS.StreamData.SchemaElement;
import com.hp.hpl.CHAOS.StreamOperator.SingleInputStreamOperator;

public class cutRNN extends SingleInputStreamOperator {

	// each operator only maintain current window and current outiler
	// Add:
	// 1. the triangle rule for fast determing the early cut-off
	// 2. the tuple prune strat

	Window_Container2 w1;// w2
	Outlier_Container o1;
	long tuplesProcessed = 0;
	Tuple_container R_Tuple;
	// each operator store one outlier detector
	//KNN_cut knn;
	HashMap rnnmap;//wycarol

	

	int id;
	// window semantic parameters
	int range, slide, slideperwindow;

	// outlier query parameters
	int k, n;

	// the schema for all input tuples is the same.
	StreamQueue inputQueue;
	SchemaElement[] schArray;
	
	// Testing related
		Runtime rt = Runtime.getRuntime();// yyw
		long memory_End;
		long execution_Start,execution_End;
	
	public cutRNN(int operatorID, StreamQueue[] input,
			StreamQueue[] output) {
		super(operatorID, input, output);
	}

	// Parse query plan,
	// read window parameters, query parameters
	// read queue schema in classVariableSetup, in run, or in class initialize
	// setup
	// when inputQueue is read?
	// Initialize Window_container here?
	@Override
	public void classVariableSetup(String key, String value) {
		if (key.equalsIgnoreCase("query")) {
			XMLVarParser parser = new XMLVarParser(value);

			range = parser.getRange();
			// System.out.println(range);
			slide = parser.getSlide();
			// System.out.println(slide);

			k = parser.getK();
			// System.out.println(k);
			n = parser.getN();
			// System.out.println(n);
			slideperwindow = (range / slide);

			// check no overlap
			/*
			 * if (window % slideLen == 0) { slideNumInWindow = window /
			 * slideLen; } else { try { throw new
			 * WindowSlideNotOverlapException(); } catch
			 * (WindowSlideNotOverlapException e) { e.printStackTrace();
			 * System.out.println("please input reasonable window and slide!");
			 * } }
			 */
			// new window_conataine here
			// if the input rate is low, the w1 will not full and will continune
			// inserting tuples
			w1 = new Window_Container2(range, slide, true);
		}

		// o1 = new Outlier_container(range);
	}

	@Override
	public int run(int maxDequeueSize) {

		Tuple_container current_tuple;

		inputQueue = getInputQueueArray()[0];
		schArray = inputQueue.getSchema();

		// output result put here?
		// initialize o1

		// initialize the supporting tuple_containers

		// check the strategy of processing stream
		// System.out.println("maxDequeueSize:\t"+maxDequeueSize+"\n");
		// System.out.println("QueueSize:\t"+inputQueue.getSize()+"\n");

		// Main loop, now assume MaxDequeueSize is > QueueSize, so this operator
		// will
		// only be called twice, window logic in now build inside using a check
		// loop
		for (int i = maxDequeueSize; i >= 0; i--) {

			// long execution_Start = (new Date()).getTime();
			// System.out.println("Execution Start:" + execution_Start);

			tuplesProcessed++;
			byte[] event = inputQueue.dequeue();

			if (event == null)
				break;

			for (SchemaElement sch : schArray)
				sch.setTuple(event);

			// System.out.println(schArray.length);

			// event to tuple_container, should parser this according to schema
			current_tuple = new Tuple_container(event, schArray);

			// insert tuple first and check if it's full
			// System.out.println(w1.getSize());
			w1.insert(current_tuple);
			// System.out.println("insert a tuple; "+i);

			if (w1.isfull()) {
				// window is ready,calculate outlier
				// execution_Start = (new Date()).getTime(); // start time. // /yyw

				
				
				/*
				 * wycarol
				 */
				/*
				
				if (!w1.isInit()) {
					knn = new KNN_cut(w1, k, n);
					w1.set_init();
				} else {//wycarol what's the difference between if and else?
					// knn.update(w1);
					knn = new KNN_cut(w1, k, n);
				}
				*/
				
				
				
				//double[] temp_arr = { 0.50, 0.50 };// wycarol so
				// this is a
				// randomly
				// choosed
				// tuple?
				//R_Tuple = new Tuple_container(2, 0, temp_arr);// wycarol public
				// Tuple_container(int  
				// att_num, int index,
				// double[] Atts) the
				// dimention is 5?should
				// it be set by the xml?

				// wycarol the sort time
				execution_Start = (new Date()).getTime(); // start time. // /yyw
				//R_sort(R_Tuple);
				//new cutRNN_function(w1, k, n).R_sort(R_Tuple);

				execution_Start = (new Date()).getTime(); // start time. // /yyw
				rnnmap = (new cutRNN_function(w1, k, n)).RNN_determine_all();

				execution_End = (new Date()).getTime(); // end time /yyw
				rt.gc();
				memory_End = rt.totalMemory() - rt.freeMemory();
				printTimeandMemory(execution_End - execution_Start, memory_End);// print
				//System.out.println(rnnmap);


				
			//	for(i=0;i<w1.getSize();i++){
			//	System.out.println(w1.ind_g[i]+" ");
			//	}
				/*
				for(int m=0;m<w1.getRange()-1;m++){
					Tuple_container tt1=w1.getTuple_g(m);
					for(int n=0;n<w1.getRange()-1;n++){
						Tuple_container tt2=w1.getTuple_g(n);
						double aa=calDistance(tt1,tt2);
						
						System.out.println(tt1.getIndex());
						System.out.println(tt2.getIndex());
						System.out.println(aa);
						
					}
				}
				*/
	//			System.out.println();		
//				System.out.println("totaltupleProcessed:\t" + tuplesProcessed);
//				System.out.println("Knn Outliers Brutal Force with cut");
//				System.out.println(o1);				
				w1.expire();
				
			}
		}
		return 0;
	}
	
	// Utility
		private void printTimeandMemory(long time, long memory) {

			// System.out.println("CPU time: "+time);
			System.out.print(time + "\t");
			DecimalFormat df = new DecimalFormat("0.00#");

			// System.out.println("Memorty consumption: "+ df.format((memory/1024.0
			// ))+"KB");
			System.out.println(df.format((memory / 1024.0)) );
		}
	
		
		private void printTimeandMemory(String funname, long time, long memory) {

			// System.out.println("CPU time: "+time);
			System.out.print("Current Process:"+funname+"\t");
			System.out.print(time + "\t");
			DecimalFormat df = new DecimalFormat("0.00#");

			// System.out.println("Memorty consumption: "+ df.format((memory/1024.0
			// ))+"KB");
			System.out.println(df.format((memory / 1024.0)));
		}
		public double calDistance(Tuple_container t1, Tuple_container t2) {
			double distance = 0.00;
			for (int i = 0; i < t1.getAtt_num(); i++) {
				distance += Math.pow((t1.getAtt(i) - t2.getAtt(i)), 2);
			}
			return Math.sqrt(distance);
		}

}
