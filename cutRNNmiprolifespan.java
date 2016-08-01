  package com.hp.hpl.CHAOS.RNN.copy;

import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.hp.hpl.CHAOS.Aggregation.Configure;
import com.hp.hpl.CHAOS.Queue.StreamQueue;
import com.hp.hpl.CHAOS.StreamData.SchemaElement;
import com.hp.hpl.CHAOS.StreamOperator.SingleInputStreamOperator;

public class cutRNNmiprolifespan extends SingleInputStreamOperator {

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
	List<Tuple_container> tuples1;
	
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
	
	public cutRNNmiprolifespan(int operatorID, StreamQueue[] input,
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

		Tuple_container current_tuple;//(wycarol)

		inputQueue = getInputQueueArray()[0];
		schArray = inputQueue.getSchema();// (Y) schArray 

		
	
		
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

			for (SchemaElement sch : schArray)//(Y)?
				sch.setTuple(event);

			// System.out.println(schArray.length);

			// event to tuple_container, should parser this according to schema
			current_tuple = new Tuple_container(event, schArray);
			// System.out.println(current_tuple.getIndex());
			// System.out.println(current_tuple.getAtts()[0]);

			// tuple_container to slide_container, no need for brutal force one
			/*
			 * 
			 * 
			 * if still in slide current_slide.enque(current_tuple) else
			 * new_slide = new new_slide.enque()
			 * 
			 * if window_container is not full
			 * window_slide_container.enque(new_slide) else
			 * window_slide_con.deque enque
			 */

			// tuple_container to window_container, this is only need for brutal
			// force one
			w1.insert(current_tuple);

			if (w1.isfull()) {
				execution_Start = (new Date()).getTime(); // start time. // /yyw

				if (!w1.isInit()) {
					rnnmap = new cutRNNmiprolifespan_function(w1, k, n)
							.RNN_determine_all();
					w1.set_init();
					execution_End = (new Date()).getTime(); // end time /yyw
					rt.gc();
					memory_End = rt.totalMemory() - rt.freeMemory();
					printTimeandMemory(execution_End - execution_Start,
							memory_End);// print

				  //  System.out.println(rnnmap);
				    
				    
				    /*
					int j,jj;
					
					for (j = slideperwindow - 1; j >= 0; j--) {

						tuples1 = w1.a.subList(w1.slide_mark[j],
								w1.slide_mark[j + 1]);
						int eachslide = 0;
						// int before=0;

						for (jj = w1.getSlide() - 1; jj >= 0; jj--) {
							System.out.println(tuples1.get(jj).getIndex());
						   
							System.out.println(tuples1.get(jj).unsafe1);
						}
					}
					*/
				   /*
				    int kkk=0;
				    for(int check1=0;check1<=w1.getSlideperrange()-1;check1++){
				    	for(int check2=0;check2<=w1.getSlide()-1;check2++){
				    		 System.out.println(w1.getTuple_l(check1, check2).getIndex());
				    		
				    	}
				    }
				    System.out.println("11111111111");
				    
				    for(int check3=0;check3<=w1.getRange()-1;check3++){
				    	System.out.println(w1.getTuple_g(check3).getIndex());
				    }
				  */
					
					/*
					
					int j,jj;
					
					for (j = slideperwindow - 1; j >= 0; j--) {

						tuples1 = w1.a.subList(w1.slide_mark[j],
								w1.slide_mark[j + 1]);
						int eachslide = 0;
						// int before=0;

						for (jj = w1.getSlide() - 1; jj >= 0; jj--) {
							System.out.println(tuples1.get(jj).getIndex());
						 
							System.out.println(tuples1.get(jj).evidnumap);
						}
					}
				
					*/

					// System.out.println("totaltupleProcessed:\t" +
					// tuplesProcessed);
					// System.out.println("Knn Outliers Slide-Based");
					// System.out.println(o1);
					w1.expire();
				} else {
					// knn = new KNN_Slide_cut(w1,k,n);
					// knn.update(w1);
				    int kkk=0;

				    
					rnnmap = new cutRNNmiprolifespan_function(w1, k, n)
					.minimal_probing_noninitialize();
					
					execution_End = (new Date()).getTime(); // end time /yyw   
					rt.gc();
					memory_End = rt.totalMemory() - rt.freeMemory();
					printTimeandMemory(execution_End - execution_Start,
							memory_End);// print

				  // System.out.println(rnnmap);
				   
				   /*
					int j,jj;
					
					for (j = slideperwindow - 1; j >= 0; j--) {

						tuples1 = w1.a.subList(w1.slide_mark[j],
								w1.slide_mark[j + 1]);
						int eachslide = 0;
						// int before=0;

						for (jj = w1.getSlide() - 1; jj >= 0; jj--) {
							System.out.println(tuples1.get(jj).getIndex());
						   
							System.out.println(tuples1.get(jj).unsafe1);
						} 
					}
				   
*/

					w1.expire();
				}

				/*
				rnnmap = new BFlifespanRNN_function(w1, k, n)
						.minimal_probing_all1();
				execution_End = (new Date()).getTime(); // end time /yyw
				rt.gc();
				memory_End = rt.totalMemory() - rt.freeMemory();
				printTimeandMemory(execution_End - execution_Start, memory_End);// print

				System.out.println(rnnmap);

				// System.out.println("totaltupleProcessed:\t" +
				// tuplesProcessed);
				// System.out.println("Knn Outliers Slide-Based");
				// System.out.println(o1);
				w1.expire();
				*/
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
