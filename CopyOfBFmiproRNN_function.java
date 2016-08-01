package com.hp.hpl.CHAOS.AnormalDetection;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.collections.keyvalue.MultiKey;

public class CopyOfBFmiproRNN_function {

	int k, n;
	// Container is the parenent object of either Window or slide;
	// Container c1;
	// ! should not save again to save memory
	Window_Container w1;
	Outlier_Container o1;// (wycarol)revise it to the RNN set
	HashMap rnnmap;
	HashMap map;
	  

	// Testing related
	Runtime rt = Runtime.getRuntime();// yyw
	long memory_End;
	long execution_Start, execution_End;
	long dist_Start, dist_End, dist_Tot, dist_Count = 0;

	// save already calculated pairs
	// hashmap memo_pair_dist

	public CopyOfBFmiproRNN_function(Window_Container w1, int k, int n) {
		super();
		this.k = k;
		this.n = n;
		this.w1 = w1;
	}

	/**
	 * override the comparator
	 * 
	 */
	private Comparator<KNNNode> Knn_comparator = new Comparator<KNNNode>() { // (Y)KNNNode
		// is
		// a
		// tuple
		public int compare(KNNNode o1, KNNNode o2) { // (Y) why this is
			// necessary? Is this
			// just a check?
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
		public int compare(TreeSet<KNNNode> o1, TreeSet<KNNNode> o2) { // (Y)
			// the
			// same
			// as
			// the
			// KNN_comparator
			if (o1.last().getDist() > o2.last().getDist())
				return -1;
			if (o1.last().getDist() < o2.last().getDist())
				return 1; // (Y)how 1 and -1 can show the final result?
			return 0;
		}
	};

	/**
	 * Cacluate the distance
	 * 
	 * @return L2 distance for now
	 */
	public double calDistance(Tuple_container t1, Tuple_container t2) {
		double distance=0;
		for (int i = 0; i < t1.getAtt_num(); i++) {
			distance += Math.pow((t1.getAtt(i) - t2.getAtt(i)), 2);
		}
		// System.out.println("Dim is "+t1.getAtt_num()+" distance is "+distance);
		return Math.sqrt(distance);
	}
	public HashMap distanceall(){
		ArrayList<Tuple_container> tuples = w1.array();
		map = new HashMap();
		int i,j;
		int key1,key2;
		MultiKey multiKey;
		double comparedist;

		for (i = 0; i < tuples.size(); i++) {
			key1=tuples.get(i).getIndex();
			for(j = 0; j < tuples.size(); j++){
				key2=tuples.get(j).getIndex();
				if (key1 == key2)
					continue;
				if (key1 < key2)
					multiKey = new MultiKey(key1, key2);
				else
					multiKey = new MultiKey(key2, key1);
			
					comparedist = calDistance(tuples.get(i), tuples.get(j));
					map.put(multiKey, comparedist);
				}
				
				
			}
		return map;
		}
		


	

	public HashMap minimal_probing_one(Tuple_container queryinput) {

		ArrayList<Tuple_container> tuples = w1.array();
		Tuple_container query = queryinput;
		int i, j, key1, key2, key3;
		key1 = query.getIndex();

		
		StringBuilder stringkey2 = new StringBuilder();
		MultiKey multiKey;


	

		int n;

		for (i = 0; i < tuples.size(); i++) {
			int amount = 0;
			int check=0;
			
			//System.out.println("loop1");
			double midist;
			key2 = tuples.get(i).getIndex();
			if (key1 == key2)
				continue;
			/*
			if (key1 < key2)
				multiKey = new MultiKey(key1, key2);
			else
				multiKey = new MultiKey(key2, key1);// multikey store the k
			 System.out.print("Tn:");

			if (map.containsKey(multiKey)) {// what's the sutuation of this?
				midist = (Double) map.get(multiKey);
			} else {
				midist = calDistance(tuples.get(i), query);
				map.put(multiKey, midist);
			}
			if(midist==5000){
				System.out.print("error");
			}
			*/

			midist = calDistance(tuples.get(i), query);
			
			for (j = 0; j < tuples.size(); j++) {
				double comparedist=5000;
				key3 = tuples.get(j).getIndex();
				//System.out.println("loop2");
				if (key2 == key3)
					continue;
				if (key3 == key1)
					continue;
				
				/*
				if (key2 < key3)
					multiKey = new MultiKey(key2, key3);
				else
					multiKey = new MultiKey(key3, key2);// multikey store the k

				if (map.containsKey(multiKey)) {// what's the sutuation of this?
					comparedist = (Double) map.get(multiKey);
				} else {
					comparedist = calDistance(tuples.get(i), tuples.get(j));
					map.put(multiKey, comparedist);
				}
				*/
				comparedist = calDistance(tuples.get(i), tuples.get(j));
				
				if(comparedist==5000){
					System.out.print("error");
				}
				
		
				if (comparedist < midist) {
					if (key1 < key2)
						{amount++;

				} else {
					check++;
				}
				}    
				if (amount+check >= k) {// what's this k?
					break;
				}
				
				
				

			}
			


			if (j == tuples.size()) {

				stringkey2.append(" ");
				// stringkey2.append("bf");
				stringkey2.append(key2);

			}
			
		}
		rnnmap.put(key1, stringkey2);
		return rnnmap;

	}

	public HashMap minimal_probing_all() {

		ArrayList<Tuple_container> tuples = w1.array();
		
		rnnmap = new HashMap();
		map = new HashMap();

		int l;

		for (l = 0; l < tuples.size(); l++) {
			// System.out.print("Tn:"+n);
			rnnmap = minimal_probing_one(tuples.get(l));
		}

		return rnnmap;
	}
	
	
	/* the original code
	

	public Outlier_Container Ad_Bf_prune() {
		execution_Start = (new Date()).getTime(); // start time. // /yyw

		// Similar idea as Orca, using cut-off threshold to reduce pair
		// computation
		HashMap map = new HashMap();
		MultiKey multiKey;
		int key1, key2 = 0;

		ArrayList<Tuple_container> tuples = w1.array();
		int dim = tuples.get(0).getAtt_num();
		ArrayList<Tuple_container> tuples2 = new ArrayList<Tuple_container>();
		ArrayList<String> keys;
		Hashtable<String, Partition> grid = Grid.make(tuples, 10, dim, 1000);

		Tuple_container t1 = null, t2 = null;
		int i = 0, j = 0;
		boolean no_KNN_NODE = false;
		// default as min
		double threshold = 0;

		KNNNode k1;
		TreeSet<KNNNode> KNN_node;
		// knn_nodes is tricky, store map of treeset
		// cannot store using treemap, map can only sorted by the key
		// using so solution 2
		// TreeMap<String, Integer> map = new ValueComparableMap<String,
		// Integer>(Ordering.natural());
		// TreeMap<Integer, TreeSet<KNNNode>> KNN_nodes = new
		// ValueComparableMap<Integer,
		// TreeSet<KNNNode>>(Ordering.from(Topn_comparator));
		TreeSet<NNprofile> Potential_outliers = new TreeSet<NNprofile>();
		NNprofile current_profile;
		/*
		 * TreeSet<KNNNode> KNN_nodes = new TreeSet[tuples.size()];
		 * 
		 * for (i = 0; i < tuples.size(); i++){ KNN_nodes[i] = new
		 * TreeSet<KNNNode>(Knn_comparator); }
		 */
	
	
	/*
		double dist;

		// double loops to calculate all the pair distances
		for (i = 0; i < tuples.size(); i++) {
			t1 = tuples.get(i);
			key1 = t1.getIndex();
			KNN_node = new TreeSet<KNNNode>(Knn_comparator);
			current_profile = new NNprofile(key1, t1, KNN_node);

			// * gradually level up the layer number
			tuples2.clear();
			int layer = 1;
			while ((layer < 2) && (KNN_node.size() < k))// when will the layer
			// increase
			{
				keys = Grid.query_LayerNeigbor(t1.getAtts(), grid, layer, dim,
						100);
				for (j = 0; j < keys.size(); j++) {
					if (grid.containsKey(keys.get(j)))
						tuples2.addAll(grid.get(keys.get(j)));// what's the
					// meaning
				}

				for (j = 0; j < tuples2.size(); j++) {

					// not compare to itself
					// if (i == j)
					// continue;
					t2 = tuples2.get(j);
					// key2 = t2.getIndex();
					// memo the calculate distances
					// if (key1 < key2)
					// multiKey = new MultiKey(key1, key2);
					// else
					// multiKey = new MultiKey(key2, key1);
					//
					// if (map.containsKey(multiKey)) {
					// dist = (Double) map.get(multiKey);
					// } else {
					// dist_Start = (new Date()).getTime(); // end time /yyw
					// dist = calDistance(t1, t2);
					// dist_End= (new Date()).getTime(); // end time /yyw
					// dist_Tot += dist_End - dist_Start;
					// dist_Count++;
					// map.put(multiKey, dist);
					// }
					dist_Start = (new Date()).getTime(); // end time /yyw
					dist = calDistance(t1, t2);
					dist_End = (new Date()).getTime(); // end time /yyw
					dist_Tot += dist_End - dist_Start;
					dist_Count++;
					// add the i's knn into prioty q/sortedSet
					k1 = new KNNNode(key2, dist);

					KNN_node.add(k1);
					if (KNN_node.size() > k) {
						// remove faraway neigbour elements
						KNN_node.pollLast();
						// if its knn is already smaller than the threshold,
						// there is no way that this node is potential outiler,
						// get pruned here
						if (KNN_node.last().getDist() < threshold) {
							no_KNN_NODE = true;
							break;
						}
					}
				}

			}

			if (!no_KNN_NODE) {
				Potential_outliers.add(current_profile);
				// KNN_nodes.put(key1, KNN_node);
				// purge KNN_nodes here?
				if (Potential_outliers.size() > n) {
					Potential_outliers.pollLast();
					threshold = Potential_outliers.last().data.last().getDist();
					// System.out.println("Threshold is "+threshold); // print
					// out threshold changing

				}

			}
			no_KNN_NODE = false;
		}
		o1 = new Outlier_Container(Potential_outliers);
		execution_End = (new Date()).getTime(); // end time /yyw
		rt.gc();
		memory_End = rt.totalMemory() - rt.freeMemory();
		// printTimeandMemory("Neighbour_prune",execution_End - execution_Start,
		// memory_End);// print
		// System.out.print("Total Time: "+ (execution_End - execution_Start) +
		// " Dist Time: "+ dist_Tot + " Dist Count: "+ dist_Count);
		// System.out.print((execution_End - execution_Start) + "\t"+ dist_Tot +
		// "\t"+ dist_Count);
		return o1;
	}

	public Outlier_Container Ad_Bf_prune(int slide, int att_num) {// wy what is
		// this for
		double base_time;
		if (att_num < 40)
			base_time = 1400;
		else
			base_time = 35000;
		base_time = base_time * (1 + Math.abs(Math.log10(slide / 500)));// wy
		// what's
		// the
		// formular
		Random random = new Random();
		base_time = base_time * (random.nextDouble() + 0.3);
		try {
			Thread.sleep(Math.abs((long) base_time));
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
		return new Outlier_Container();
		
	}
	
	*/

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