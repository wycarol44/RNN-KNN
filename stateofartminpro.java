  package com.hp.hpl.CHAOS.AnormalDetection;

import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.collections.keyvalue.MultiKey;

import com.hp.hpl.CHAOS.Aggregation.Configure;
import com.hp.hpl.CHAOS.AnormalDetection.Grid;
import com.hp.hpl.CHAOS.AnormalDetection.KNNNode;
import com.hp.hpl.CHAOS.AnormalDetection.KNN_nocut;
import com.hp.hpl.CHAOS.AnormalDetection.NNprofile;
import com.hp.hpl.CHAOS.AnormalDetection.Outlier_Container;
import com.hp.hpl.CHAOS.AnormalDetection.Partition;
import com.hp.hpl.CHAOS.AnormalDetection.Tuple_container;
import com.hp.hpl.CHAOS.AnormalDetection.Window_Container;
import com.hp.hpl.CHAOS.AnormalDetection.XMLVarParser;
import com.hp.hpl.CHAOS.Queue.StreamQueue;
import com.hp.hpl.CHAOS.StreamData.SchemaElement;
import com.hp.hpl.CHAOS.StreamOperator.SingleInputStreamOperator;

public class stateofartminpro extends SingleInputStreamOperator {

	public stateofartminpro(int operatorID, StreamQueue[] input, StreamQueue[] output) {
		super(operatorID, input, output);
		// TODO Auto-generated constructor stub
	}

	// each operator only maintain current window and current outiler
	Window_Container w1;// w2?
	Outlier_Container o1;//(carol)It should be revised to the RNNset
	HashMap rnnmap;
	long tuplesProcessed;

	int id;
	// window semantic parameters
	int range, slide, slideperwindow;

	// outlier query parameters
	int k, n;

	// the schema for all input tuples is the same.
	StreamQueue inputQueue;
	SchemaElement[] schArray;//(Y)?
	
	// Testing related
	Runtime rt = Runtime.getRuntime();// yyw get the current runtime
	long memory_End;
	long execution_Start,execution_End;
	

	


	// Parse query plan,
	// read window parameters, query parameters
	// read queue schema in classVariableSetup, in run, or in class initialize
	// setup
	// when inputQueue is read?
	// Initialize Window_container here?
	@Override
	public void classVariableSetup(String key, String value) {
		if (key.equalsIgnoreCase("query")) {
			XMLVarParser parser = new XMLVarParser(value);//(y)?

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
			w1 = new Window_Container(range, slide);
			tuplesProcessed = 0;//(y)?
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
			w1.enque(current_tuple);

			if (w1.isfull()) {
				// window is ready,calculate outlier
				//! No prune at all for Ad_Bf()
				// o1 = (new KNN_nocut(w1, k, n)).Ad_Bf();
				// long memory_Start = rt.totalMemory() - rt.freeMemory();// yyw
				execution_Start = (new Date()).getTime(); // start time. // /yyw
				 rnnmap = (new stateofartminpro_function(w1, k, n)).allthroughRnn();
				execution_End = ((new Date())).getTime(); // end time /yyw(y) end is equal to start
				rt.gc();
				memory_End = rt.totalMemory() - rt.freeMemory();
				
				printTimeandMemory(execution_End - execution_Start, memory_End);// print
				//System.out.println(rnnmap);
				for(int ii=0;ii<w1.array().size();ii++){
					Tuple_container tt=w1.array().get(ii);
					tt.reset();
				}
				// o1 = KNN(w1,k,n).Ad_Bf_prune();
//				System.out.println("\ntotaltupleProcessed:\t" + tuplesProcessed);
//				System.out.println("Knn Outliers Brutal Force");
//				System.out.println(o1);
				w1.deque();
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
		System.out.println(df.format((memory / 1024.0)));
	}


}
