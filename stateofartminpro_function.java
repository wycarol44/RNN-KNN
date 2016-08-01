package com.hp.hpl.CHAOS.AnormalDetection;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections.keyvalue.MultiKey;

import com.google.common.collect.Ordering;
import com.hp.hpl.CHAOS.Queue.StreamQueue;
import com.hp.hpl.CHAOS.AnormalDetection.KNNNode;
import com.hp.hpl.CHAOS.AnormalDetection.NNprofile;
import com.hp.hpl.CHAOS.AnormalDetection.Tuple_container;
import com.hp.hpl.CHAOS.StreamData.SchemaElement;

public class stateofartminpro_function {

	int k, n;
	Window_Container w;
	Outlier_Container o;
	Tuple_container R_Tuple;
	public double threshold = 0;
	HashMap rnnmap;// wycarol
	HashMap map;
	HashMap knnmap;
	HashMap orderknnmap;
	HashMap orderdismap;

	// Testing related
	Runtime rt = Runtime.getRuntime();// yyw
	long memory_End;
	long execution_Start, execution_End;
	long dist_Start, dist_End, dist_Tot, dist_Count = 0;

	// wycarol

	// prune_stop indicate the stop position
	// private int prune_stop = 0;

	public stateofartminpro_function(Window_Container w, int k, int n) {
		this.k = k;
		this.n = n;
		this.w = w;

		List<Tuple_container> data_tuples, query_tuples;
		// generate the reference tuple
		// R_Tuple = Pick_Tuple("manual");
		// double[] temp_arr = { 500.0, 500.0 };
		// R_Tuple = new Tuple_container(2, 0, temp_arr);

		// double[] temp_arr = { 500.0, 500.0, 500.0, 500.0, 500.0, 500.0,500.0,
		// 500.0,500.0, 500.0, 500.0, 500.0, 500.0, 500.0, 500.0, 500.0,500.0,
		// 500.0,500.0, 500.0};
		// R_Tuple = new Tuple_container(20, 0, temp_arr);

		double[] temp_arr = { 0.50, 0.50 };// wycarol so
		// this is a
		// randomly
		// choosed
		// tuple?
		R_Tuple = new Tuple_container(2, 0, temp_arr);// wycarol public
		// Tuple_container(int
		// att_num, int index,
		// double[] Atts) the
		// dimention is 5?should
		// it be set by the xml?

		// wycarol the sort time
		//execution_Start = (new Date()).getTime(); // start time. // /yyw
		// R_sort(R_Tuple);
		allthrough();
		// R_sort_global(R_Tuple);
		//execution_End = (new Date()).getTime(); // end time /yyw
		//rt.gc();// wycarol what's this? the gc is not found
		//memory_End = rt.totalMemory() - rt.freeMemory();

		//execution_Start = (new Date()).getTime(); // start time. // /yyw
		//// RNN_determine_main();
		//execution_End = (new Date()).getTime(); // end time /yyw
		//rt.gc();// wycarol what's this? the gc is not found
		//memory_End = rt.totalMemory() - rt.freeMemory();
		//printTimeandMemory(null, execution_End - execution_Start, memory_End);

	}

	// run nl_cut_0_0.xml
	public void allthrough() {
		Iterator<Tuple_container> windowtuple=w.array().iterator();
		while(windowtuple.hasNext()) {
			Tuple_container query =windowtuple.next();
			int query1 = query.getIndex();
			//TreeSet<KNNNode> KNN_nodee;
			//KNN_nodee = new TreeSet<KNNNode>(Knn_comparator);
		//	execution_Start = (new Date()).getTime();
			Iterator<Tuple_container> window=w.array().iterator();
			while(window.hasNext()) {

				Tuple_container tuple = window.next();
				// System.out.println(query.tuplequeue);
				// System.out.println(query.distancequeue);
				int tuple1 = tuple.getIndex();
				if (tuple1 == query1)
					continue;
				//execution_Start = (new Date()).getTime(); // start time. // /yyw
				calDistance(query, tuple);
				//execution_End = (new Date()).getTime(); // end time /yyw
				//rt.gc();// wycarol what's this? the gc is not found
				//memory_End = rt.totalMemory() - rt.freeMemory();
				//printTimeandMemory(null, execution_End - execution_Start, memory_End);

				// System.out.println(query.getIndex());
				// System.out.println(query.tuplequeue);
				// System.out.println(query.distancequeue);
			}
		//	execution_End = (new Date()).getTime(); // end time /yyw
		//	rt.gc();// wycarol what's this? the gc is not found
		//	memory_End = rt.totalMemory() - rt.freeMemory();
		//	printTimeandMemory(null, execution_End - execution_Start, memory_End);
			// System.out.println(query.getIndex());
			//System.out.println(query.tuplequeue0);
			//System.out.println(query.tuplequeue1);
			//System.out.println(query.tuplequeue2);
			//System.out.println(query.tuplequeue3);
			//System.out.println(query.tuplequeue4);
			//System.out.println(query.tuplequeue5);
		}
	}

	// Helper Functions, should merge into the father class
	//
	//
	//

	/**
	 * Cacluate the distance
	 * 
	 * @return L2 distance for now TODO: 1. Lp distance 2. Hamming distance L1 ?
	 *         Matrix distance will help?
	 * 
	 * 
	 * 
	 */


	public void calDistance(Tuple_container t1, Tuple_container t2) {
		KNNNode k2;

		double distance = 0.00;
		double x = t1.getAtt(0) - t2.getAtt(0);
		double y = t1.getAtt(1) - t2.getAtt(1);
		double z = y / x;
		for (int i = 0; i < t1.getAtt_num(); i++) {
			distance += Math.pow((x - y), 2);
		}
		distance = Math.sqrt(distance);
		int zone;
		int key1=t1.getIndex();
		int key2=t2.getIndex();
		k2 = new KNNNode(t2, distance);

		if (x >= 0 && y >= 0) {
			if (z >= 0.5) {
				zone = 0;

				t1.tuplequeue0.add(k2);
				if (t1.tuplequeue0.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue0.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			} else {
				zone = 5;
				t1.tuplequeue5.add(k2);
				if (t1.tuplequeue5.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue5.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			}

		}
		if (x <= 0 && y >=0) {
			if (z <= -0.5) {
				zone = 1;
				t1.tuplequeue1.add(k2);
				if (t1.tuplequeue1.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue1.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			} else {
				zone = 2;
				t1.tuplequeue2.add(k2);
				if (t1.tuplequeue2.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue2.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			}

		}
		if (x <= 0 && y <= 0) {
			if (z >= 0.5) {
				zone = 3;
				t1.tuplequeue3.add(k2);
				if (t1.tuplequeue3.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue3.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			} else {
				zone = 2;
				t1.tuplequeue2.add(k2);
				if (t1.tuplequeue2.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue2.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			}

		}		
		if (x >= 0 && y <= 0) {
			if (z <= -0.5) {
				zone = 4;
				t1.tuplequeue4.add(k2);
				if (t1.tuplequeue4.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue4.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			} else {
				zone = 5;
				t1.tuplequeue5.add(k2);
				if (t1.tuplequeue5.size() > k) {
					// remove faraway neigbour elements
					t1.tuplequeue5.pollLast();
				}
				return;
				//KeepTheQueue(zone, t1, t2, distance);
			}

		}
	}

	// wycarol: the order distance of knn of a certain tuple
	// function to get the threshold?:

	// wycarol this is a function:the disp computation(sort the disp in each
	// slide)

	// ////////////////

	// updating local sorted index
	public HashMap allthroughRnn() {
		HashMap rnnmap = new HashMap();
		for (int tt = 0; tt < w.array().size() - 1; tt++) {
			com.hp.hpl.CHAOS.AnormalDetection.Tuple_container query = w.array()
					.get(tt);
		

			//System.out.println(query.getIndex() + "+");
			StringBuilder stringkey2 = new StringBuilder();
			int count=0;
			Iterator<KNNNode> zero=query.tuplequeue0.iterator();
			Iterator<KNNNode> one=query.tuplequeue1.iterator();
			Iterator<KNNNode> two=query.tuplequeue2.iterator();
			Iterator<KNNNode> three=query.tuplequeue3.iterator();
			Iterator<KNNNode> four=query.tuplequeue4.iterator();
			Iterator<KNNNode> five=query.tuplequeue5.iterator();
			while(zero!=null&&zero.hasNext()) {
				
				Tuple_container candidate=zero.next().t;
				//count++;
				//System.out.println(tupleloop.getIndex());
				rnnmap = ReturnRnn(query, candidate, stringkey2, rnnmap);// return
				//System.out.println(count);															// the
															// candidates
			}
				
				while(one!=null&&one.hasNext()) {
				
					Tuple_container candidate=one.next().t;
					//count++;
					//System.out.println(tupleloop.getIndex());
					rnnmap = ReturnRnn(query, candidate, stringkey2, rnnmap);// return
					//System.out.println(count);															// the
																// candidates
				}
				while(two!=null&&two.hasNext()) {
					
					Tuple_container candidate=two.next().t;
					//count++;
					//System.out.println(tupleloop.getIndex());
					rnnmap = ReturnRnn(query, candidate, stringkey2, rnnmap);// return
					//System.out.println(count);															// the
																// candidates
				}
				while(three!=null&&three.hasNext()) {
					
					Tuple_container candidate=three.next().t;
					//count++;
					//System.out.println(tupleloop.getIndex());
					rnnmap = ReturnRnn(query, candidate, stringkey2, rnnmap);// return
					//System.out.println(count);															// the
																// candidates
				}
				while(four!=null&&four.hasNext()) {
					
					Tuple_container candidate=four.next().t;
					//count++;
					//System.out.println(tupleloop.getIndex());
					rnnmap = ReturnRnn(query, candidate, stringkey2, rnnmap);// return
					//System.out.println(count);															// the
																// candidates
				}
				while(five!=null&&five.hasNext()) {
					
					Tuple_container candidate=five.next().t;
					//count++;
					//System.out.println(tupleloop.getIndex());
					rnnmap = ReturnRnn(query, candidate, stringkey2, rnnmap);// return
					//System.out.println(count);															// the
																// candidates
				}

			
		}
		return rnnmap;
	}
	

	//run nl_cut_0_0.xml
	

	public HashMap ReturnRnn(Tuple_container query, Tuple_container tuplee,
			StringBuilder stringkey2, HashMap rnnmap) {

		// not using threshold parameters, utterly calculate every pairs then
		// rank
		
		// ArrayList<TreeSet> RNNresult = new ArrayList<TreeSet>();
		HashMap map = new HashMap();
		MultiKey multiKey;

		// a hash map to store the rnn
		MultiKey multiKey2;// using the same method as the above
		int distcompare;// using this to compare the distance between (t1,t2)
		// and the knn value

		int  key2;
		// ArrayList key3=new ArrayList();
		ArrayList<Tuple_container> tuples = w.array();
		Tuple_container t1 = tuplee, t2 = null;
		int key1=tuplee.getIndex();

		int i = 0, j = 0;

		KNNNode k1;
		TreeSet<KNNNode> KNN_node;

		TreeSet<NNprofile> Potential_outliers = new TreeSet<NNprofile>();

		// TreeSet<NNprofile> t3 = Potential_outliers;

		NNprofile current_profile;
        int key3=query.getIndex();
		double dist = 0;
		// run nl_cut_0_0.xml
		// double loops to calculate all the pair distances
        double line=dist = calDistance2(query, tuplee);

		KNN_node = new TreeSet<KNNNode>(Knn_comparator);// does knn node
		// have a value now?
		current_profile = new NNprofile(key1, t1, KNN_node);
		int count=0;
		for (j = 0; j < tuples.size(); j++) {
			// not compare to itself

			t2 = tuples.get(j);
			key2 = t2.getIndex();
			if (key1 == key2)
				continue;

				dist = calDistance2(t1, t2);
          if(dist<line){
        	  count++;
          }
          if(count>=k){break;}
			


		}

		// (wycarol)
  		if (j==tuples.size()&&count<k) {

			stringkey2.append(" ");
			stringkey2.append(key1);
			rnnmap.put(key3, stringkey2);
		}



		return rnnmap;

	}

	public double calDistance2(Tuple_container t1, Tuple_container t2) {
		double distance = 0.00;
		for (int i = 0; i < t1.getAtt_num(); i++) {
			distance += Math.pow((t1.getAtt(i) - t2.getAtt(i)), 2);
		}
		// System.out.println("Dim is "+t1.getAtt_num()+" distance is "+distance);
		return Math.sqrt(distance);
	}

	/***
	 * override the comparator
	 * 
	 * */
	private Comparator<KNNNode> Knn_comparator = new Comparator<KNNNode>() {
		public int compare(KNNNode o1, KNNNode o2) {
			if (o1.getDist() >= o2.getDist()) {
				return 1;
			} else {
				return -1;
			}
		}
	};

	/**
	 * override the comparator
	 */
	private Comparator<TreeSet<KNNNode>> Topn_comparator = new Comparator<TreeSet<KNNNode>>() {
		@Override
		public int compare(TreeSet<KNNNode> o1, TreeSet<KNNNode> o2) {
			if (o1.last().getDist() > o2.last().getDist())
				return -1;
			if (o1.last().getDist() < o2.last().getDist())
				return 1;
			return 0;
		}
	};

	// wycarol what's this class for?
	public Outlier_Container Ad(int slide, int att_num) {
		double base_time;
		if (att_num < 40)
			base_time = 190;
		else
			base_time = 3000;
		base_time = base_time * (1 + Math.abs(Math.log10(slide / 500)));
		Random random = new Random();
		base_time = base_time * (random.nextDouble() + 0.3);
		try {
			Thread.sleep(Math.abs((long) base_time));
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		return new Outlier_Container();
	};

	private void printTimeandMemory(String funname, long time, long memory) {

		// System.out.println("CPU time: "+time);
		System.out.print("Current Process:" + funname + "\t");
		System.out.print(time + "\t");
		DecimalFormat df = new DecimalFormat("0.00#");

		// System.out.println("Memorty consumption: "+ df.format((memory/1024.0
		// ))+"KB");
		System.out.println(df.format((memory / 1024.0)) + "\t");
	}

}
