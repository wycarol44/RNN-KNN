package com.hp.hpl.CHAOS.RNN.copy;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections.keyvalue.MultiKey;

import com.google.common.collect.Ordering;

public class cutRNNmipro_function {

	int k, n;
	Window_Container2 w;
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

	public cutRNNmipro_function(Window_Container2 w, int k, int n) {
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
		execution_Start = (new Date()).getTime(); // start time. // /yyw
		// R_sort(R_Tuple);
		R_sort(R_Tuple);
		// R_sort_global(R_Tuple);
		execution_End = (new Date()).getTime(); // end time /yyw
		rt.gc();// wycarol what's this? the gc is not found
		memory_End = rt.totalMemory() - rt.freeMemory();

		execution_Start = (new Date()).getTime(); // start time. // /yyw
		// RNN_determine_main();
		execution_End = (new Date()).getTime(); // end time /yyw
		rt.gc();// wycarol what's this? the gc is not found
		memory_End = rt.totalMemory() - rt.freeMemory();
		// printTimeandMemory("R_Sort",execution_End - execution_Start,
		// memory_End);// print

		// wycarol the threshold computation time
		// wycarol
		/*
		 * execution_Start = (new Date()).getTime(); // start time. // /yyw
		 * threshold = cut_off(); execution_End = (new Date()).getTime(); // end
		 * time /yyw rt.gc(); memory_End = rt.totalMemory() - rt.freeMemory();
		 * // printTimeandMemory("Cut_off",execution_End - execution_Start,
		 * memory_End);// print
		 * 
		 * 
		 * // System.out.println("pre_cut-off = " + threshold); //
		 * System.out.println("the max distance = " + //
		 * w.getKTupleDist_g(w.getSize()-1)); //
		 * System.out.println("the max distance = " + //
		 * w.getKTupleDist_g(w.getSize()/2));
		 * 
		 * execution_Start = (new Date()).getTime(); // start time. // /yyw //
		 * int layer = 1; // while ((layer < 2) & data_tuples.size()){
		 * 
		 * // }
		 * 
		 * //wycarol time to get the data_tuples and query_tuples // while layer
		 * data_tuples = w.a.subList(0, w.a.size()); query_tuples =
		 * prune_tuple(data_tuples); execution_End = (new Date()).getTime(); //
		 * end time /yyw rt.gc(); memory_End = rt.totalMemory() -
		 * rt.freeMemory(); // printTimeandMemory("Tuple_prune",execution_End -
		 * execution_Start, memory_End);// print //
		 * System.out.println("query_size = "+query_tuples.size());
		 * 
		 * 
		 * //wycarol time of prune_neighbor function execution_Start = (new
		 * Date()).getTime(); // start time. // /yyw o =
		 * prune_neighbour(data_tuples, query_tuples);//wycarol o is a outlier
		 * container so this should be the outcome?it use the class below?
		 * execution_End = (new Date()).getTime(); // end time /yyw rt.gc();
		 * memory_End = rt.totalMemory() - rt.freeMemory(); //
		 * printTimeandMemory("Neighbour_prune",execution_End - execution_Start,
		 * memory_End);// print
		 * 
		 * // System.out.println("post_cut-off = " + threshold); //
		 * System.out.print("Total Time: "+ (execution_End - execution_Start) +
		 * " Dist Time: "+ dist_Tot + " Dist Count: "+ dist_Count); //
		 * System.out.print((execution_End - execution_Start) + "\t"+ dist_Tot +
		 * "\t"+ dist_Count);
		 */

	}

	private Outlier_Container prune_neighbour(
			List<Tuple_container> data_tuples,
			List<Tuple_container> query_tuples) {
		// Loops through query tuples and output
		// query tuples in 3 groups:
		// 1. potential outliers,
		// 2. safe inliers,
		// 3. unsafe inliers
		// wycarol so this is the step to get the final result? :yes the output
		// is the n outliers

		Outlier_Container o1;// wycarol instead we can use this to store the RNN

		double dist;
		int key1, key2;
		// Tuple_container t1 = null, t2 = null;
		boolean no_KNN_NODE = false;
		KNNNode k1;
		TreeSet<KNNNode> KNN_node;
		// TreeMap<Integer, TreeSet<KNNNode>> KNN_nodes = new
		// ValueComparableMap<Integer, TreeSet<KNNNode>>(
		// Ordering.from(Topn_comparator));

		TreeSet<NNprofile> Potential_outliers = new TreeSet<NNprofile>();
		NNprofile current_profile;

		for (Tuple_container q : query_tuples) {
			key1 = q.getIndex();// wycarol the original index
			KNN_node = new TreeSet<KNNNode>(Knn_comparator);
			current_profile = new NNprofile(key1, q, KNN_node);

			// * instead pruning the whole data_tuples, using grid will limit it
			// to only
			// computing

			for (Tuple_container d : data_tuples) {
				// this step can reuse memozation
				key2 = d.getIndex();
				if (key1 == key2)
					continue;
				dist_Start = (new Date()).getTime(); // end time /yyw
				dist = calDistance(q, d);
				dist_End = (new Date()).getTime(); // end time /yyw
				dist_Tot += dist_End - dist_Start;
				dist_Count++;

				k1 = new KNNNode(key2, dist);
				KNN_node.add(k1);// wycarol compute the knn of key1 and make the
				// result(key2,dist)

				if (KNN_node.size() > k) {
					// wycarol maintain the smallest k as neighbors
					// remove faraway neigbour elements
					KNN_node.pollLast();
					// if its knn is already smaller than the threshold,
					// there is no way that this node is potential outiler,
					// get pruned here
					if (KNN_node.last().getDist() < threshold) {// wycarol using
						// the threshold
						// to determine
						// the state of
						// the tuple
						no_KNN_NODE = true;// wycarol what's this?
						break;
					}
				}
			}
			if (!no_KNN_NODE) {// wycarol if the knn value is larger than the
				// threshold(no_KNN_NODE is false)
				Potential_outliers.add(current_profile);// wycarol add it to the
				// protential_ourliers
				// container
				// KNN_nodes.put(key1, KNN_node);
				// purge KNN_nodes here?
				if (Potential_outliers.size() > n) { // wycarol make the
					// potential ourlier
					// container the size of
					// n(in fact the
					// potential outlier
					// container is the
					// ourlier container?)
					Potential_outliers.pollLast();
					threshold = Potential_outliers.last().data.last().getDist();// wycarol
					// is
					// the
					// last
					// tuple's
					// KNN
					// value
				}

			}
			no_KNN_NODE = false;// wycarol make the unlabeled one false
		}

		o1 = new Outlier_Container(Potential_outliers);
		return o1;
	}

	private List<Tuple_container> prune_tuple(List<Tuple_container> data_tuples) {// wycarol
		// the
		// stopping
		// rule?
		double Xk = w.getKTupleDist_g(k - 1);// wycarol is the
		// w.getKTupleDist_g(k - 1) the
		// kth tuple in the disp index?
		List<Tuple_container> pruned_tuples = new ArrayList<Tuple_container>();

		for (int i = data_tuples.size() - 1; i >= 0; i--) {
			if ((w.getKTupleDist_g(i) + Xk) < threshold) {
				// prune_stop = i;
				break;
			} else {
				// pruned_tuples store the tuple in the sorted order
				pruned_tuples.add(w.getTuple_g(i));
			}
		}
		return pruned_tuples;// just need to consider the pruned_tuples
	}

	public void update(Window_Container2 w) {// wycarol what's this update for?
		// :the pruned_tuples is the
		// only one that need to be
		// labeled
		this.w = w;
		R_sort(R_Tuple);// wycarol sort the original ?this is a tuple?

		// rerun all the occurrence
		threshold = cut_off();

		List<Tuple_container> data_tuples, query_tuples;
		data_tuples = w.a.subList(0, w.a.size());
		query_tuples = prune_tuple(data_tuples);

		o = prune_neighbour(data_tuples, query_tuples);

	}

	// wycarol is this necessary?
	public Outlier_Container Ad() {
		// TODO Auto-generated method stub
		return o;
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
	 */
	public double calDistance(Tuple_container t1, Tuple_container t2) {
		double distance = 0.00;
		for (int i = 0; i < t1.getAtt_num(); i++) {
			distance += Math.pow((t1.getAtt(i) - t2.getAtt(i)), 2);
		}
		return Math.sqrt(distance);
	}

	// wycarol: the order distance of knn of a certain tuple
	// function to get the threshold?:
	private double cut_off_global(int i) {
		// for each tuples, finding at most 2k neighbor tuple
		// for each tuple, get the order distance knn of each tuple
		// wycarol the disp sort is written in this function?
		// wycarol why need to find so much tuples? in case it expire?
		// wycarol reduce the computation
		int j, left, right, end;
		// ArrayList<Double> l_dist = new ArrayList<Integer>();
		// ArrayList<Double> r_dist = new ArrayList<Integer>();
		double Xt, Xcl, Xcr;
		double kth;

		ArrayList<Double> topn = new ArrayList<Double>();
		// wycarol
		/*
		 * 
		 * Xt = w.getKTupleDist_g(0); // assume range >> 2k // assume range >> n
		 * kth = w.getKTupleDist_g(k) - Xt;//wycarol the distance bewteen the
		 * reference and the kth Disp point topn.add(kth);//wycarol add what?why
		 * add this?
		 */

		// wycarol
		// for (i = 1; i < w.getSize(); i++) {//wycarol compute the order
		// distance
		left = i - 1;
		right = i + 1;
		end = right;
		j = 0;
		Xt = w.getKTupleDist_g(i);// wycarol this is the value of the tuple?
		while ((j < k) && (left >= 0) && (right < w.getSize())) {// wycarol what
			// to do
			// with the
			// tuples
			// between 0
			// and k?
			Xcl = Math.abs(Xt - w.getKTupleDist_g(left));
			Xcr = Math.abs(w.getKTupleDist_g(right) - Xt);
			if (Xcl < Xcr) {
				end = left;
				left--;
			} else {
				end = right;
				right++;
			}
			j++;
		}
		if (j < k) {
			if (left < 0)
				end = right + (k - j);
			else if (right >= w.getSize())
				end = left - (k - j);
		}
		kth = Math.abs(w.getKTupleDist_g(end) - Xt);

		// wycarol
		/*
		 * topn.add(kth); //wycarol}
		 * 
		 * Collections.sort(topn);// wycarol what's this function?
		 */

		return kth;// wycarol return the initial threshold?
	}

	// the

	private double cut_off() {
		// for each tuples, finding at most 2k neighbor tuple
		// wycarol the disp sort is written in this function?
		// wycarol why need to find so much tuples? in case it expire?
		// wycarol reduce the computation
		int i, j, left, right, end;
		// ArrayList<Double> l_dist = new ArrayList<Integer>();
		// ArrayList<Double> r_dist = new ArrayList<Integer>();
		double Xt, Xcl, Xcr;
		double kth;
		ArrayList<Double> topn = new ArrayList<Double>();

		Xt = w.getKTupleDist_g(0);
		// assume range >> 2k
		// assume range >> n
		kth = w.getKTupleDist_g(k) - Xt;// wycarol the distance bewteen the
		// reference and the kth Disp point
		topn.add(kth);// wycarol add what?why add this?

		for (i = 1; i < w.getSize(); i++) {// wycarol compute the order distance
			left = i - 1;
			right = i + 1;
			end = right;
			j = 0;
			Xt = w.getKTupleDist_g(i);// wycarol this is the value of the tuple?
			while ((j < k) && (left >= 0) && (right < w.getSize())) {// wycarol
				// what
				// to do
				// with
				// the
				// tuples
				// between
				// 0 and
				// k?
				Xcl = Math.abs(Xt - w.getKTupleDist_g(left));
				Xcr = Math.abs(w.getKTupleDist_g(right) - Xt);
				if (Xcl < Xcr) {
					end = left;
					left--;
				} else {
					end = right;
					right++;
				}
				j++;
			}
			if (j < k) {
				if (left < 0)
					end = right + (k - j);
				else if (right >= w.getSize())
					end = left - (k - j);
			}
			kth = Math.abs(w.getKTupleDist_g(end) - Xt);
			topn.add(kth);
		}

		Collections.sort(topn);// wycarol what's this function?
		return topn.get(n);// wycarol return the initial threshold?
	}

	// wycarol this is a function:the disp computation(sort the disp in each
	// slide)
	private void R_sort(Tuple_container r_Tuple) {
		// wycarol what's the return of this function?

		// wycarol where is the sort in this6 function?
		// wycarol so the return of this function is only the resorted window
		// which has both local and global index
		int i, j;
		int slot = 0, base = 0, offset = 0;

		Tuple_container temp;
		// get globally sorted index
		// sorted each slide
		temp = w.a.get(0);
		temp.dist = calDistance(temp, r_Tuple);
		// w.ind_g.clear();

		// wycarol both the first tuple's global and the local index is 0
		w.ind_g[0] = 0;// wycarol global sort index
		w.ind_l.get(0)[0] = 0;// wycarol local sort index;why this is a array?

		for (i = 1; i < w.getSize(); i++) {
			temp = w.a.get(i);// wycarol the ith tuple
			temp.dist = calDistance(temp, r_Tuple);// wycarol which is the loop?
			// using this index to mark the relative position
			for (j = 0; j < i; j++) {
				if (w.getKTupleDist_g(j) > temp.dist) {// wycarol what's this
					// criteria?
					break;
				}
			}
			int temp_id = j;
			int temp_id1 = w.ind_g[j];
			int temp_id2;
			if (j < i) {
				for (; j < i; j++) {
					temp_id2 = w.ind_g[j + 1];
					w.ind_g[j + 1] = temp_id1;
					temp_id1 = temp_id2;

				}
			}
			w.ind_g[temp_id] = i;

			// for local sorted index
			// for tuple based
			// if (w.count_based) {
			slot = (i / w.getSlide());// wycarol what's this?
			base = (slot * w.getSlide());// wycarol what's this?
			offset = i - base;// what's this fomular?
			// }
			/*
			 * if (offset == 0) { w.ind_l.get(slot)[0] = 0; } else { for (j = 0;
			 * j < i - base; j++) { if (w.getKTupleDist_l(slot, j) > temp.dist)
			 * { break; } } w.ind_l.get(slot)[j] = offset;// wycarol slot is the
			 * label of // slide } }
			 */
		}

		// updating local sorted index

	}

	// updating local sorted index

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

	public HashMap RNN_determine_onetuple(Tuple_container queryinput,
			int count, int n) {

		Tuple_container query = queryinput;
		boolean stoprule;
		boolean determined = false;
		int i;
		int key1, key2;
		key1 = query.getIndex();
		int up = n - 1;
		int down = n + 1;
		int label = 0;
		int countno = 0;
		// run nl_cut_0_0.xml
		MultiKey multiKey;
		double dist;
		double knnvalue;
		double orderknn;
		double orderdist;
		StringBuilder stringkey2 = new StringBuilder();
		// System.out.print("rn:"+key1);
		double Dis_ref_k = Math
				.abs(w.getKTupleDist_g(k) - w.getKTupleDist_g(0));

		while (determined == false && countno != w.getRange() - 1) {
			// System.out.print("rn:"+i);

			if (n != w.getRange() - 1) {
				if (n == 0) {
					i = down;
					down++;
					countno++;
				} else {
					if (up == -1) {
						label = 0;
					}
					if (down == w.getRange()) {
						label = 1;
					}
					if (label == 1) {
						label = 0;
						i = up;
						up--;
						countno++;
					} else {
						label = 1;
						i = down;
						down++;
						countno++;
					}
				}

			} else {
				i = up;
				up--;
				countno++;
			}

			key2 = w.getTuple_g(i).getIndex();
			// stringkey2.append("abnormal");
			// rnnmap.put(key2,null);
			if (key1 == key2)
				continue;

			dist = calDistance(w.getTuple_g(i), query);
			orderdist = Math.abs(w.getKTupleDist_g(i) - w.getKTupleDist_g(0));

			orderdist = Math.abs(w.getKTupleDist_g(i) - w.getKTupleDist_g(0));
			stoprule = ((orderdist + Dis_ref_k) < dist);
			if (!stoprule || count <= k + 1) {

				orderknn = cut_off_global(i);
				boolean Isrnn;
				Isrnn = dist < orderknn;// 2k computation
				if (Isrnn) {
					stringkey2.append(" ");
					// stringkey2.append("isr");
					stringkey2.append(key2);
					rnnmap.put(key1, stringkey2);
					// System.out.print("*1*");
					continue;
				} else {

					rnnmap=minimal_probing(query, w.getTuple_g(i), i,stringkey2);
					continue;
				}

			} else {
				if (down != w.getRange()) {
					continue;
				}

				determined = true;

			}

		}

		return rnnmap;
	}

	public HashMap minimal_probing(Tuple_container query,
			Tuple_container key2query, int key2position,StringBuilder stringkey2) {
		int key1 = query.getIndex();
		int key2 = key2query.getIndex();
		int h;
		int upp = key2position - 1;
		int downn = key2position + 1;
		int labell =0; 
		int countnoo = 0;
		int total=0;
		int totalafter=0;
		double midist = calDistance(key2query, query);

		while (total < k && countnoo != w.getRange() - 1) {

			if (key2position != w.getRange() - 1) {
				if (key2position == 0) {
					h = downn;
					downn++;
					countnoo++;
				} else {
					if (upp == -1) {
						labell = 0;
					}
					if (downn == w.getRange()) {
						labell = 1;
					}
					if (labell == 1) {
						labell = 0;
						h = upp;
						upp--;
						countnoo++;
					} else {
						labell = 1;
						h = downn;
						downn++;
						countnoo++;
					}
				}

			} else {
				h = upp;
				upp--;
				countnoo++;
			}
			
				
				double comparedist = 5000;
				int key3 = w.getTuple_g(h).getIndex();
				if (key2 == key3 ) {
					continue;
				}
				if (key3 == key1 ) {
					continue;
				}
				comparedist = calDistance(w.getTuple_g(h),
						key2query);

				if (comparedist == 5000) {
					System.out.print("error");
				}
				if (comparedist < midist) {
					total++;

				}

				if (total >= k) {// what's this k?
					break;
				}
				if(total<k&&countnoo == w.getRange() - 2){
					stringkey2.append(" ");
					// stringkey2.append("bf");
					stringkey2.append(key2);
				}
				

		}
		rnnmap.put(key1, stringkey2);
		return rnnmap;
	}

	
	
	public HashMap RNN_determine_all() {
		rnnmap = new HashMap();

		int n;

		int count = w.getRange();
		for (n = w.getRange() - 1; n >= 0; n--) {

			rnnmap = RNN_determine_onetuple(w.getTuple_g(n), count, n);
			count--;
			// System.out.print(rnnmap);

		}
		// System.out.println(knnmap);

		return rnnmap;

	}

	private Tuple_container initialize_original(Tuple_container T1) {

		MultiKey multiKey;

		MultiKey multiKey2;// using the same method as the above
		int distcompare;// using this to compare the distance between (t1,t2)
		// and the knn value

		int key1, key2;
		// ArrayList key3=new ArrayList();
		ArrayList<Tuple_container> tuples = w.a;

		int i = 0, j = 0;

		TreeSet<KNNNode> KNN_node;
		// knn_nodes is tricky, store map of treeset
		// cannot store using treemap, map can only sorted by the key
		// using so solution 2
		// TreeMap<String, Integer> map = new ValueComparableMap<String,
		// Integer>(Ordering.natural());
		// TreeMap<Integer, TreeSet<KNNNode>> KNN_nodes = new
		// ValueComparableMap<Integer,
		// TreeSet<KNNNode>>(Ordering.from(Topn_comparator));

		// override the previouse hashmap storage with new oop

		// TreeSet<NNprofile> t3 = Potential_outliers;

		NNprofile current_profile;

		/*
		 * TreeSet<KNNNode> KNN_nodes = new TreeSet[tuples.size()];
		 * 
		 * for (i = 0; i < tuples.size(); i++){ KNN_nodes[i] = new
		 * TreeSet<KNNNode>(Knn_comparator); }
		 */
		double dist = 0;

		// double loops to calculate all the pair distances

		Tuple_container t1 = T1;
		key1 = t1.getIndex();
		KNN_node = new TreeSet<KNNNode>(Knn_comparator);// does knn node
		// have a value now?
		current_profile = new NNprofile(key1, t1, KNN_node);
		for (j = 0; j < tuples.size(); j++) {
			Tuple_container t2 = tuples.get(j);
			key2 = t2.getIndex();
			// not compare to itself
			if (key1 == key2)
				continue;

			dist = calDistance(t1, t2);
			// add the i's knn into prioty q/sortedSet
			KNNNode k1 = new KNNNode(key2, dist);

			KNN_node.add(k1);
			if (KNN_node.size() > k) {
				// remove faraway neigbour elements
				KNN_node.pollLast();
			}
			t1.nn_g = KNN_node;
			// return t1;

		}
		return t1;
	}
}
