package com.hp.hpl.CHAOS.RNN.copy;

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

import com.hp.hpl.CHAOS.RNN.copy.Tuple_container;

public class CopyOfupdateBFlifespanRNN2_function {

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

	public CopyOfupdateBFlifespanRNN2_function(Window_Container2 w1, int k, int n) {
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
		
		for (i = 0; i < slideperwindow; i++) {
			detect_tuples = w1.a
					.subList(w1.slide_mark[i], w1.slide_mark[i + 1]);
			for (ii = 0; ii < w1.getSlide(); ii++) {
				key2 = detect_tuples.get(ii).getIndex();
				ArrayList eviforkey2 = new ArrayList();// final evidence by
				Old_Evidence savekey2 = new Old_Evidence(key2);					// slide for key2
				if (key1 == key2) {
		

					continue;
				}
				int total = 0;
				int totalafter = 0;
				int count = 0;
				// System.out.println("loop1");
				double midist;
				midist = calDistance(detect_tuples.get(ii), query);

				for (j = slideperwindow - 1; j >= 0; j--) {

					evidence_tuples = w1.a.subList(w1.slide_mark[j],
							w1.slide_mark[j + 1]);
					int eachslide = 0;
					boolean added = false;
					ArrayList tempeviforkey2 = new ArrayList();
					for (jj = w1.getSlide() - 1; jj >= 0; jj--) {
						count++;
						double comparedist = 5000;
						key3 = evidence_tuples.get(jj).getIndex();
						if (key2 == key3 ) {
							if(jj==0){
							tempeviforkey2.add(eachslide);
							tempeviforkey2.addAll(eviforkey2);
							eviforkey2 = tempeviforkey2;
							added = true;
							}
							continue;
						}
						if (key3 == key1 ) {
							if(added==false&& jj==0){
								tempeviforkey2.add(eachslide);
								tempeviforkey2.addAll(eviforkey2);
								eviforkey2 = tempeviforkey2;
								added = true;
								}
							continue;
						}
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
						if (added==false&&jj == 0 ) {
							tempeviforkey2.add(eachslide);
							tempeviforkey2.addAll(eviforkey2);
							eviforkey2 = tempeviforkey2;
							added = true;
						}
						if (total >= k) {// what's this k?
							if (added == false ) {

								tempeviforkey2.add(eachslide);
								tempeviforkey2.addAll(eviforkey2);
								eviforkey2 = tempeviforkey2;
								added = true;

							}
							// detect_tuples.get(ii).setInlier();// is not rnn

							if (totalafter < k && i != 0) {
								// query.unsafe.get(j).add(key2);//the slide j's
								// unsafe key2s


								savekey2.setindex(key2);
								savekey2.settotal(total);
								savekey2.settotalafter(totalafter);
								savekey2.setevidencenumber(eviforkey2);
								query.unsafe1.get(j).add(savekey2);
								//System.out.println(eviforkey2);

							}
						// wycarol problem
							break;
						}
					}
					if (added == false && total < k ) {

						tempeviforkey2.add(eachslide);
						tempeviforkey2.addAll(eviforkey2);
						eviforkey2 = tempeviforkey2;
						added = true;
					}
					if (total >= k) {
						break;
					}
					
				}
				if (count == w1.getSize()&& total < k) {
					
					stringkey2.append(" ");
					// stringkey2.append("bf");
					stringkey2.append(key2);

					if (i != 0) {
						savekey2.setindex(key2);
						savekey2.settotal(total);
						savekey2.settotalafter(totalafter);
						savekey2.setevidencenumber(eviforkey2);
						query.unsafe1.get(0).add(savekey2);
						//System.out.println(eviforkey2);
					}
				}
				

			}

		}

		rnnmap.put(key1, stringkey2);

		return rnnmap;

	}

	public HashMap minimal_probing_all1() {// ********initialize to compute all
		// the tuples

		// List<Tuple_container> tuples;
		rnnmap = new HashMap();

		for (int y = 0; y < w1.getRange(); y++) {
			Tuple_container tuple = w1.a.get(y);

			rnnmap = minimal_probing_one1(tuple);
			//System.out.println("evidencenumber" + "/n" + tuple.evidencenumber);
			//System.out.println("unsafe" + "/n" + tuple.unsafe1);
			//System.out.println(tuple.evidnumap);

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
			
			//System.out.println("evidencenumber" + "/n" + tuple.evidencenumber);
			//System.out.println("unsafe" + "/n" + tuple.unsafe1);
			//System.out.println(tuple.evidnumap);

		}

		return rnnmap;
	}

	public HashMap minimal_probing_noninitialize() {
		rnnmap = new HashMap();
		rnnmap = minimal_probing_old();
		rnnmap = minimal_probing_new();
		
		return rnnmap;
	}

	public HashMap minimal_probing_old() {// ******** to compute all the new
		// tuples in old slide

		List<Tuple_container> tuples;
		int y, x;
		tuples = w1.a.subList(w1.slide_mark[0], w1.slide_mark[w1
				.getSlideperrange() - 1]);

		for (x = w1.getRange() - w1.getSlide() - 1; x >= 0; x--) {
			Tuple_container tuple = tuples.get(x);

			rnnmap = minimal_probing_complex_update(tuple, x);
			//System.out.println("evidencenumber" + "/n" + tuple.evidencenumber);
			//System.out.println("unsafe" + "/n" + tuple.unsafe1);
			//System.out.println(tuple.evidnumap);
			// System.out.print("test"+tuple.getIndex());
			// System.out.print(tuple.evidence+"\n");
		}
		return rnnmap;
	}

	public HashMap minimal_probing_complex_update(Tuple_container tuple,int x) {
		//System.out.print("original"+tuple.getIndex());
	//	System.out.print(tuple.evidence+"\n");
		ArrayList nukk = new ArrayList();
		tuple.unsafe1.add(nukk);
		int key3;
	    int key1=tuple.getIndex();
		
		int label=-1;
		List<Tuple_container> newtuples = w1.a.subList(w1.slide_mark[slideperwindow-1], w1.slide_mark[slideperwindow]);
		
		StringBuilder stringkey2 = new StringBuilder();

		
		int startuple=w1.a.get(0).getIndex();//the index of the first tuple in the window

		
//****************************************************************************************************
		//situation1:update the new key2 tuples with the whole window
		
		
		// to find other evidences
		
		   // List<ArrayList> sublist=tuple.evidence.subList(0, w1.getRange()-w1.getSlide());
		   // ArrayList<ArrayList> biglist= new ArrayList<ArrayList>();
            for (int j = w1.getSlide()-1; j >=0; j--) {//**********collecting evidence in for the new tuples

			
			Tuple_container newtuple = newtuples.get(j);
			int key2 = newtuple.getIndex();// the key2(tuples) in
			Old_Evidence savekey2 = new Old_Evidence(key2);	
			// the original window



			int totalafter=0;
			int total =0;
			int count1=0;
			ArrayList eviforkey2 = new ArrayList();
			
			double midist = calDistance(newtuple, tuple);		
			//tuple.evidence.remove(0);

		// int before=0;

			for (int q = slideperwindow - 1; q >= 0; q--) {

				List<Tuple_container>evidence_tuples = w1.a.subList(w1.slide_mark[q],
						w1.slide_mark[q + 1]);
				int eachslide = 0;
				boolean added = false;

				// int before=0;
				ArrayList tempeviforkey2 = new ArrayList(); 
				for (int tt = w1.getSlide() - 1; tt >= 0; tt--) {

					count1++;

					double comparedist = 5000;
					key3 = evidence_tuples.get(tt).getIndex();

					// eviforkey2.add("key2:"+key2);
					// System.out.println("loop2");
					if (key2 == key3) {
						if(tt==0){
							tempeviforkey2.add(eachslide);
							tempeviforkey2.addAll(eviforkey2);
							eviforkey2 = tempeviforkey2;
							added = true;
							}
						continue;
					}

					if (key3 == key1) {
						if(added==false&&tt==0){
							tempeviforkey2.add(eachslide);
							tempeviforkey2.addAll(eviforkey2);
							eviforkey2 = tempeviforkey2;
							added = true;
							}

						continue;
					}

					comparedist = calDistance(newtuple,
							evidence_tuples.get(tt));

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
					if (added==false&&tt == 0) {                         
                        tempeviforkey2.add(eachslide); 
                        tempeviforkey2.addAll(eviforkey2); 
                        eviforkey2=tempeviforkey2; 
                        added = true;                            
                    } 
                  

					if (total >= k) {// what's this k?

						if (added == false) { 
                            
                            tempeviforkey2.add(eachslide); 
                            tempeviforkey2.addAll(eviforkey2); 
                            eviforkey2=tempeviforkey2; 
                            added = true; 
                          
                        } 

                        if (totalafter < k){ 
                            //query.unsafe.get(j).add(key2);//the slide j's unsafe key2s 
                        	savekey2.setindex(key2);
							savekey2.settotal(total);
							savekey2.settotalafter(totalafter);
							savekey2.setevidencenumber(eviforkey2);
							tuple.unsafe1.get(q+1).add(savekey2);
                                  
                        }                        
                                                                       
                        break; 
					}
				}

				if (total >= k) {
					break;
				}




				if (count1==w1.getSize()&& total <k) {
					stringkey2.append(" ");
					// stringkey2.append("bf");
					stringkey2.append(key2);
					
					
					savekey2.setindex(key2);
					savekey2.settotal(total);
					savekey2.settotalafter(totalafter);
					savekey2.setevidencenumber(eviforkey2);
					tuple.unsafe1.get(1).add(savekey2);

				}



			}




			}





		//**************************************************************************************************************
		
		//****************************************************************************************************
		//situation2 the key2 are old tuples, they need to update the evidence
            int v=tuple.unsafe1.get(0).size();
               for(int w=0;w<v;w++){ 
            	   Old_Evidence ooooo=tuple.unsafe1.get(0).get(w);
                int key2= ooooo.getindex();//the label num in the current window(start from 1-range) 
                // the key2(tuples) in 
            // the original window 
          
  
            //ArrayList sublistw = new ArrayList(); 

            
       
            int total1 = ooooo.gettotal();//original one 
           
            ArrayList<Integer> oldevidence=(ArrayList) ooooo.getevidencenumber();//original one 
            int Nofslideneedtoprobe=oldevidence.size()-1; 
           
            int evidencexpire=oldevidence.get(0); 
            
             
            ArrayList eviforkey2=new ArrayList();
            eviforkey2 = oldevidence; 
            eviforkey2.remove(0);
            Tuple_container tuplekey2=w1.a.get(key2-startuple); 
            double midist = calDistance(tuplekey2, tuple); 
  
          
            // eviforkey2=tuple.evidence.get(label); 
  
            int totalafter1 =ooooo.gettotalafter(); 
            total1=total1-evidencexpire; 
  
               
            for(int h=slideperwindow - 1;h>=Nofslideneedtoprobe;h--){
            	List<Tuple_container>evidence_tuples = w1.a.subList(w1.slide_mark[h], 
                        w1.slide_mark[h + 1]); 
                int eachslide = 0; 
                boolean added = false; 
                ArrayList tempeviforkey2 = new ArrayList(); 
				for (int t = w1.getSlide() - 1; t >= 0; t--) {

				

					double comparedist = 5000;
					key3 = evidence_tuples.get(t).getIndex();

					// eviforkey2.add("key2:"+key2);
					// System.out.println("loop2");
					if (key2 == key3) {
						if(t==0){
							tempeviforkey2.add(eachslide);
							tempeviforkey2.addAll(eviforkey2);
							eviforkey2 = tempeviforkey2;
							added = true;
							}
						continue;
					}

					if (key3 == key1) {
						if(added=false&&t==0){
							tempeviforkey2.add(eachslide);
							tempeviforkey2.addAll(eviforkey2);
							eviforkey2 = tempeviforkey2;
							added = true;
							}
						continue;
					}

					comparedist = calDistance(tuplekey2,
							evidence_tuples.get(t));

					if (comparedist == 5000) {
						System.out.print("error");
					}

					if (comparedist < midist) {
						total1++;
						eachslide++;
					    totalafter1++;

						 // else {
						// before++;

						// }
					}
					if (added==false&&t == 0) {                         
                        tempeviforkey2.add(eachslide); 
                        tempeviforkey2.addAll(eviforkey2); 
                        eviforkey2=tempeviforkey2; 
                        added = true;                            
                    } 
                  

					if (totalafter1 >= k) {// what's this k?

           
                                                                       
                        break; 
					}
					if (h==Nofslideneedtoprobe&&t==0 &&total1 <k  ) {
						stringkey2.append(" ");
						// stringkey2.append("bf");
						stringkey2.append(key2);
						
						
						if(key2>=startuple+w1.getSlide()){
							
							ooooo.settotal(total1);
							ooooo.settotalafter(totalafter1);
							ooooo.setevidencenumber(eviforkey2);
							tuple.unsafe1.get(1).add(ooooo);
						}

					}
					if (h==Nofslideneedtoprobe&&t==0 &&total1 >=k) {
					
						if(key2>=startuple+w1.getSlide()){
							ooooo.settotal(total1);
							ooooo.settotalafter(totalafter1);
							ooooo.setevidencenumber(eviforkey2);
							tuple.unsafe1.get(1).add(ooooo);
						}

					}
				}

				if (totalafter1 >= k) {
					break;
				}




				
                
                
                
                
            }
            
            
            
            
            
            
            
            }
		
    		tuple.unsafe1.remove(0);
    		rnnmap.put(key1, stringkey2);
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
