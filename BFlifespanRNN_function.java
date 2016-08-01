package com.hp.hpl.CHAOS.AnormalDetection;

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

public class BFlifespanRNN_function {

	int k, n;
	// Container is the parenent object of either Window or slide;
	// Container c1;
	// ! should not save again to save memory
	Window_Container2 w1;
	Outlier_Container o1;// (wycarol)revise it to the RNN set
	HashMap rnnmap;
	HashMap map;
	HashMap labelmap;
	int slideperwindow;// wycarol the number of slide per window

	// Testing related
	Runtime rt = Runtime.getRuntime();// yyw
	long memory_End;
	long execution_Start, execution_End;
	long dist_Start, dist_End, dist_Tot, dist_Count = 0;

	// save already calculated pairs
	// hashmap memo_pair_dist

	public BFlifespanRNN_function(Window_Container2 w1, int k, int n) {
		super();
		this.k = k;
		this.n = n;
		this.w1 = w1;
		this.slideperwindow = w1.getSlideperrange();
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
		double distance = 0;
		for (int i = 0; i < t1.getAtt_num(); i++) {
			distance += Math.pow((t1.getAtt(i) - t2.getAtt(i)), 2);
		}
		// System.out.println("Dim is "+t1.getAtt_num()+" distance is "+distance);
		return Math.sqrt(distance);
	}

	// The distance check function
	public HashMap distanceall() {
		ArrayList<Tuple_container> tuples = w1.a;
		map = new HashMap();
		int i, j;
		int key1, key2;
		MultiKey multiKey;
		double comparedist;

		for (i = 0; i < tuples.size(); i++) {
			key1 = tuples.get(i).getIndex();
			for (j = 0; j < tuples.size(); j++) {
				key2 = tuples.get(j).getIndex();
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

	public HashMap minimal_probing_one1(Tuple_container queryinput) {// make
		// this
		// as
		// the
		// initialize************
		// window

		// ArrayList<Tuple_container> tuples = w1.a;
		Tuple_container query = queryinput;
		int i, j, key1, key2, key3, ii, jj = 100;
		key1 = query.getIndex();

		StringBuilder stringkey2 = new StringBuilder();
		MultiKey multiKey;
		List<Tuple_container> detect_tuples, evidence_tuples;

		int n;
		ArrayList<ArrayList> eviforkey1 = new ArrayList<ArrayList>();

		for (i = slideperwindow - 1; i >= 0; i--) {
			detect_tuples = w1.a
					.subList(w1.slide_mark[i], w1.slide_mark[i + 1]);

			for (ii = w1.getSlide() - 1; ii >= 0; ii--) {
				// if (detect_tuples.get(ii).isSafe){
				// continue;
				// }
				key2 = detect_tuples.get(ii).getIndex();

				ArrayList eviforkey2 = new ArrayList();

				//eviforkey2.add("key2:" + key2);
				// Tuple_container lili=w1.a.get(key2);//wycarol using this to
				// store the evidence number in each slide
				if (key1 == key2){
					
					eviforkey2.add("0");
					eviforkey2.add("0");
					eviforkey2.add("0");
					eviforkey1.add(eviforkey2);
					continue;
					}
			

				int total = 0;
				int totalafter = 0;
				int count = 0;

				// System.out.println("loop1");
				double midist;

				/*
				 * if (key1 < key2) multiKey = new MultiKey(key1, key2); else
				 * multiKey = new MultiKey(key2, key1);// multikey store the k
				 * System.out.print("Tn:");
				 * 
				 * if (map.containsKey(multiKey)) {// what's the sutuation of
				 * this? midist = (Double) map.get(multiKey); } else { midist =
				 * calDistance(tuples.get(i), query); map.put(multiKey, midist);
				 * } if(midist==5000){ System.out.print("error"); }
				 */

				midist = calDistance(detect_tuples.get(ii), query);

				for (j = slideperwindow - 1; j >= 0; j--) {

					evidence_tuples = w1.a.subList(w1.slide_mark[j],
							w1.slide_mark[j + 1]);
					int eachslide = 0;
					boolean added = false;

					// int before=0;

					for (jj = w1.getSlide() - 1; jj >= 0; jj--) {

						count++;

						double comparedist = 5000;
						key3 = evidence_tuples.get(jj).getIndex();

						// eviforkey2.add("key2:"+key2);
						// System.out.println("loop2");
						if (key2 == key3) {
							if (jj == 0) {
								eviforkey2.add(eachslide);
								// eviforkey2.add(key2, eachslide);
							}
							continue;
						}

						if (key3 == key1) {
							if (jj == 0) {
								eviforkey2.add(eachslide);
								// eviforkey2.add(key2, eachslide);
							}
							continue;
						}

						/*
						 * if (key2 < key3) multiKey = new MultiKey(key2, key3);
						 * else multiKey = new MultiKey(key3, key2);// multikey
						 * store the k
						 * 
						 * if (map.containsKey(multiKey)) {// what's the
						 * sutuation of this? comparedist = (Double)
						 * map.get(multiKey); } else { comparedist =
						 * calDistance(tuples.get(i), tuples.get(j));
						 * map.put(multiKey, comparedist); }
						 */
						comparedist = calDistance(detect_tuples.get(ii),
								evidence_tuples.get(jj));

						if (comparedist == 5000) {
							System.out.print("error");
						}

						if (comparedist < midist) {
							total++;
							eachslide++;
							if (i <= j) {
								totalafter++;

							} // else {
							// before++;

							// }
						}

						if (jj == 0) {
							eviforkey2.add(eachslide);
							// eviforkey2.add(key2, eachslide);
							added = true;

							// detect_tuples.get(ii).evidence.set(j, eachslide);
							/*
							 * detect_tuples.get(ii).evidence.add("tuple:");
							 * detect_tuples.get(ii).evidence.add(ii);
							 * detect_tuples.get(ii).evidence.add("slide:");
							 * detect_tuples.get(ii).evidence.add(j);
							 * detect_tuples
							 * .get(ii).evidence.add("No.eachslide:");
							 * detect_tuples.get(ii).evidence.add(eachslide);
							 */
						}
						if (total >= k) {// what's this k?
							if (added == false) {
								eviforkey2.add(eachslide);

								added = true;
							}
							//detect_tuples.get(ii).setInlier();// is not rnn

							//if (totalafter == k) {
							//	detect_tuples.get(ii).setSafe();// is not rnn
								// until it
								// expire
							//}
							for (int w = j - 1; w >= 0; w--) {
								eviforkey2.add("0");
								// System.out.println("here");

							}
							eviforkey1.add(eviforkey2);// wycarol problem
							break;
						}
						

					}

					if (added = false) {
						eviforkey2.add("%:" + eachslide);
						added = true;
					}

					if (total >= k) {
						break;
					}

				}
				if (count == w1.getSize() && total != k) {
					eviforkey1.add(eviforkey2);
					stringkey2.append(" ");
					// stringkey2.append("bf");
					stringkey2.append(key2);

				}
				query.settotal(key2, total);
				query.settotalafter(key2, totalafter);

				// System.out.println(detect_tuples.get(ii).evidence);

			}
			// System.out.println(detect_tuples.get(ii).evidence);
		}

		/*
		 * for (j = slideperwindow - 1; j >= 0; j--) {
		 * 
		 * evidence_tuples = w1.a.subList(w1.slide_mark[j], w1.slide_mark[j +
		 * 1]); int eachslide = 0; // int before=0;
		 * 
		 * for (jj = w1.getSlide() - 1; jj >= 0; jj--) {
		 * System.out.println(evidence_tuples.get(jj).evidence); } }
		 */

		query.evidence=eviforkey1;
		rnnmap.put(key1, stringkey2);
		return rnnmap;

	}

	public HashMap minimal_probing_all1() {// ********initialize to compute all
											// the tuples

		// List<Tuple_container> tuples;
		rnnmap = new HashMap();

		// map = new HashMap();
		// labelmap = new HashMap();
		// Tuple_container tupleone;

		int y;

		for (y = 0; y < w1.getRange(); y++) {
			Tuple_container tuple = w1.a.get(y);
			rnnmap = minimal_probing_one1(tuple);

		}

		return rnnmap;
	}

	public HashMap minimal_probing_new() {// ******** to compute all the new
											// tuples in new slide

		List<Tuple_container> tuples;
		tuples = w1.a.subList(w1.slide_mark[w1.getSlideperrange() - 1],
				w1.slide_mark[w1.getSlideperrange()]);

		int y;

		for (y = w1.getSlide() - 1; y >= 0; y--) {
			Tuple_container tuple = tuples.get(y);
			rnnmap = minimal_probing_one1(tuple);// the same as the tuples in
													// the first window\
		//	System.out.print("test"+tuple.getIndex());
		//	System.out.print(tuple.evidence+"\n");

		}

		return rnnmap;
	}
	public HashMap minimal_probing_noninitialize() {
		rnnmap = new HashMap();
		rnnmap=minimal_probing_new();
		rnnmap=minimal_probing_old();
		return rnnmap; 
	}
	

	public HashMap minimal_probing_old() {// ******** to compute all the new
											// tuples in old slide

		List<Tuple_container> tuples;
		int y, x;
			tuples = w1.a.subList(w1.slide_mark[0], w1.slide_mark[w1.getSlideperrange()-1]);

			for (x = w1.getRange()-w1.getSlide()-1; x >= 0; x--) {
				Tuple_container tuple = tuples.get(x);
				rnnmap = minimal_probing_complex(tuple,x);
		//		System.out.print("test"+tuple.getIndex());
		//		System.out.print(tuple.evidence+"\n");
			}
		return rnnmap;
	}
	

	

	public HashMap minimal_probing_complex(Tuple_container tuple,int x) {
		//System.out.print("original"+tuple.getIndex());
	//	System.out.print(tuple.evidence+"\n");

		int key1, key2, key3;
		String expirevidence;
		int count = w1.getSlide()-1;
		int label=-1;

		key1 = tuple.getIndex();
		StringBuilder stringkey2 = new StringBuilder();
		List<Tuple_container> newtuples = w1.a.subList(w1.slide_mark[slideperwindow-1], w1.slide_mark[slideperwindow]);

		List<Tuple_container> oldtuples=w1.a.subList(w1.slide_mark[0], w1.slide_mark[slideperwindow-1]);;

		
		ArrayList<ArrayList> newevidence=new ArrayList<ArrayList>();
		ArrayList<ArrayList> newevidence1=new ArrayList<ArrayList>();
		ArrayList<ArrayList> newevidence2=new ArrayList<ArrayList>();
		
		

		// situation 2 determine whether the tuples in the old slides are needed
		
		
		ArrayList zero=new ArrayList();
		zero.add("9");
		zero.add("9");
		zero.add("9");
		
//****************************************************************************************************
		//situation1:the key2 are new tuples.they don't have any evidence yet
		
		
		// to find other evidences
		
		   // List<ArrayList> sublist=tuple.evidence.subList(0, w1.getRange()-w1.getSlide());
		   // ArrayList<ArrayList> biglist= new ArrayList<ArrayList>();
		for (int j = w1.getSlide()-1; j >=0; j--) {//**********collecting evidence in for the new tuples

			
			Tuple_container newtuple = newtuples.get(j);
			key2 = newtuple.getIndex();// the key2(tuples) in
			// the original window
			ArrayList littlearray1 = new ArrayList();


			int totalafter=0;
			int total =0;
			int count1=0;
			
			
			double midist = calDistance(newtuple, tuple);		
			//tuple.evidence.remove(0);

		// int before=0;

			for (int q = slideperwindow - 1; q >= 0; q--) {

				List<Tuple_container>evidence_tuples = w1.a.subList(w1.slide_mark[q],
						w1.slide_mark[q + 1]);
				int eachslide = 0;
				boolean added = false;

				// int before=0;

				for (int t = w1.getSlide() - 1; t >= 0; t--) {

					count1++;

					double comparedist = 5000;
					key3 = evidence_tuples.get(t).getIndex();

					// eviforkey2.add("key2:"+key2);
					// System.out.println("loop2");
					if (key2 == key3) {

						continue;
					}

					if (key3 == key1) {

						continue;
					}

					comparedist = calDistance(newtuple,
							evidence_tuples.get(t));

					if (comparedist == 5000) {
						System.out.print("error");
					}

					if (comparedist < midist) {
						total++;
						eachslide++;
						if (q==slideperwindow - 1) {
							totalafter++;

						} // else {
						// before++;

						// }
					}

					if (total >= k) {// what's this k?
							littlearray1.add(eachslide);
						for (int w = q - 1; w >= 0; w--) {
							littlearray1.add("0");
						}
						added=true;
						break;
					}
				}

				if (total >= k) {
					break;
				}

				littlearray1.add(eachslide);



				if (count1==w1.getSize()&& total <k) {
					stringkey2.append(" ");
					// stringkey2.append("bf");
					stringkey2.append(key2);

				}



			}
			newevidence1.add(littlearray1);
			tuple.settotal(key2, total);
			tuple.settotalafter(key2, totalafter);

		//	System.out.print(j);
		//	System.out.print("testnew"+tuple.getIndex());
		//	System.out.print(newevidence+"\n");
		//	System.out.print("end");
			

			//System.out.print("testnew"+tuple.getIndex());
			//System.out.print(newevidence1+"\n");

			}





		//**************************************************************************************************************
		
		//****************************************************************************************************
		//situation2 the key2 are old tuples, they need to update the evidence
		
		
		
		for (int j = w1.getRange()-w1.getSlide()-1; j >= 0; j--) {//*******collecting evidence for the old tuples
			//System.out.print(j);
			label++;
			Tuple_container oldtuple = oldtuples.get(j);
			key2 = oldtuple.getIndex();// the key2(tuples) in
			// the original window
			ArrayList eviforkey2 = new ArrayList();
			ArrayList evidenceleft;
			//ArrayList sublistw = new ArrayList();
			if (key1 == key2){
				///eviforkey2.add("0");
				///eviforkey2.add("0");
				///eviforkey2.add("0");
				///eviforkey1.add(eviforkey2);
		//		System.out.print("here");
				newevidence2.add(zero);
				continue;
				}
				
			int totalafter1 = Integer.parseInt(tuple.gettotalafter(key2)
					.toString());
			int total1 = Integer.parseInt(tuple.gettotal(key2).toString());
			
			//**************the first situation if it is certainly not the rnn of tuple,no need to probing 
			if (totalafter1>=k) {// if it is rnn until it expire(have k
				// evidence behind it),then check
				// the next tuple
				//eviforkey2.add("*");
				//eviforkey2.add(tuple.evidence.get(label).subList(0, w1.getSlideperrange()-2));

				newevidence2.add(label, zero);
				//eviforkey2=tuple.evidence.get(label);//if it satisfies then add the original matrix to the new one
				//eviforkey1.add(eviforkey2);
				continue;
			}

			double midist = calDistance(oldtuple, tuple);

		
			// eviforkey2=tuple.evidence.get(label);

		     expirevidence = tuple.evidence.get(label).get(
			 slideperwindow - 1).toString();

			 evidenceleft=tuple.evidence.get(label);//remove the last element as it is the information for the first slide in the last window\
			 evidenceleft.remove(slideperwindow - 1);
			 int expire=Integer.parseInt((String) expirevidence);
			 total1=total1-expire;
			 
			 //************the second situation if it was not the rnn of the tuple
			//it is certainly not rnn
				//we want to find more evidence to make it safe
			 //sublistw=tuple.evidence.get(label);
			 int count2=0;
				
				for (int jj = w1.getSlide() - 1; jj >= 0; jj--) {

					Tuple_container newtuple = newtuples.get(jj);
					double comparedist = 5000;
					key3 = newtuple.getIndex();

					comparedist = calDistance(oldtuple, newtuple);

					if (comparedist == 5000) {
						System.out.print("error");
					}

					if (comparedist < midist) {
						
                        totalafter1++;
                        total1++;
                        count2++;
					}
					
					
					
					if (totalafter1 >= k) {// what's this k?

						break;
					}			

				}	
				if(total1<k){
					stringkey2.append(" ");
					// stringkey2.append("bf");
					stringkey2.append(key2);

				}

					eviforkey2.add(count2);
					eviforkey2.addAll(evidenceleft);
					newevidence2.add(eviforkey2);
					tuple.totalafter.put(key2, totalafter1);
					tuple.totalafter.put(key2, total1);


				//System.out.print("here");
	//			System.out.print("testold"+tuple.getIndex());
	//			System.out.print(newevidence+"\n");
				//	System.out.print("testold"+tuple.getIndex());
			    //    System.out.print(newevidence2+"\n");
			}
			


			
			//***********************************************************************
			

			// situation 1 determine whether the tuples in the new coming slide
			// are the rnn of the input tuple	
		newevidence.addAll(newevidence1);
		newevidence.addAll(newevidence2);
		tuple.evidence=newevidence;
		rnnmap.put(key1, stringkey2);
	   /// System.out.print("****final****"+tuple.getIndex());
		///System.out.print(tuple.evidence+"\n");
		return rnnmap;
	}

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
