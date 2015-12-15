package org.kumar.SalonTableTalk;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;















import scala.Tuple3;



//import java.util.Set;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Hello world!
 *
 */
public class SalonDataProcessor implements Serializable
{
    //private static final long serialVersionUID = -1322322139926390329L;
    private static final long serialVersionUID = -13223239926390329L;


	public void Display(SortedSet<String> sortedShingleSet){
		System.out.println("-------------------------------\n");

		for (String shingle : sortedShingleSet){
			System.out.print("|" + shingle);
		}
		System.out.println("\n\n");
	}
	
	public float JaccardSimilarity(SortedSet<String> set1, SortedSet<String> set2){
		SortedSet<String> setUnion = new TreeSet<String>(); 
		setUnion.addAll(set1);
		setUnion.addAll(set2);
		
		SortedSet<String> setIntersection = new TreeSet<String>();
		setIntersection.addAll(set1);
		setIntersection.retainAll(set2);
		
		System.out.println("Size of intersection: " + setIntersection.size());
		System.out.println("Size of Union: " + setUnion.size());

		return (float)setIntersection.size() / (float)setUnion.size();
	}
	
	public SortedSet<String> GenerateShingleSet(String document, int shingleLength){
	//public String[] GenerateShingleSet(String document, int shingleLength){
		int docLen = document.length();
		int numOfShingles = docLen - shingleLength + 1;
		SortedSet<String> sortedShingleSet = new TreeSet<String>();
		for (int i = 0; i < numOfShingles; i++){
			sortedShingleSet.add(document.substring(i, i+shingleLength));
		}
		
		return sortedShingleSet;
	}
	
	public String[] GenerateShingleSetX(String document, int shingleLength){
		SortedSet<String> sortedShingleSet = GenerateShingleSet(document, shingleLength);
		int sizeOfSortedSingleSet = sortedShingleSet.size();
		String[] shinglesArray = new String[sizeOfSortedSingleSet];
		int i = 0;
		for (String shingle : sortedShingleSet){
			shinglesArray[i] = shingle;
			i++;
		}
		return shinglesArray;
	}
	

    public static void main( String[] args )
    {
        //String dataFile = "C:\\bigData\\BigDataMeetup\\publicData\\tabletalk-torrent\\PreProcessOutput\\TEST\\tableTalkComments*.txt";
        //String shingleIdDocIdPairRDDGroupedByKeyFile = "C:\\bigData\\HadoopFreeSpark\\output\\allShingleDocIdDistinctRDDGroupedByKey";
        //String minHashNumbersFile = "C:\\bigData\\HadoopFreeSpark\\output\\minHashNumbers";
        //String shingleIdMappedToDocIdListsFile = "C:\\bigData\\HadoopFreeSpark\\output\\shingleIdMappedToDocIdLists";
        //String docIdMinHashValuePairsFile = "C:\\bigData\\HadoopFreeSpark\\output\\docIdMinHashValuePairs";
        //String candiateDocSetsFile = "C:\\bigData\\HadoopFreeSpark\\output\\candidateDocSets";


        String dataFile = "input/tableTalkComments*.txt";
        String shingleIdDocIdPairRDDGroupedByKeyFile = "output/allShingleDocIdDistinctRDDGroupedByKey";
        String minHashNumbersFile = "output/minHashNumbers";
        String shingleIdMappedToDocIdListsFile = "output/shingleIdMappedToDocIdLists";
        String docIdMinHashValuePairsFile = "output/docIdMinHashValuePairs";
        String candiateDocSetsFile = "output/candidateDocSets";
        
    	final int shingleLength=6;
        final int minHashSigLen=24;
        final int lshBlockSize=4;
        final Long lshBucketsSize=9999L;
        final SalonDataProcessor salonDataProcessor = new SalonDataProcessor();
        
        SparkConf conf = new SparkConf().setAppName("Salon Table Talk Data Processor");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> commentsRawData = sc.textFile(dataFile).cache();
        JavaRDD<String> commentsRawData = sc.textFile(dataFile);
    	
        JavaPairRDD<Long, SortedSet<String>> docIdSortedShingleSetPairRDD = commentsRawData.mapToPair(
        		new PairFunction<
        				String, // T as input
        				Long, // K as output
        				SortedSet<String> // V as output
        		>() {
        			@Override
        			public Tuple2<Long, SortedSet<String>> call(String element) {
        				String[] tokens = element.split("\\|\\|");
        				SortedSet<String> sortedShingleSet = null;
        				if (tokens.length == 1){
        					sortedShingleSet=new TreeSet<String>();
        					sortedShingleSet.add("XXX");
        				}
        				else
        				try{
        					sortedShingleSet = salonDataProcessor.GenerateShingleSet(tokens[1], shingleLength);
        				} catch(Exception e){
        					System.out.println("Exception while processing: \n" + element );
        					System.out.println(tokens.length);
        					e.printStackTrace();
        				}
        				return new Tuple2<Long, SortedSet<String>>(new Long(tokens[0]), sortedShingleSet);
        			}
        });
        
        JavaPairRDD<Long, ArrayList<Long>> minHashPairRDD = docIdSortedShingleSetPairRDD.mapToPair(
        		new PairFunction<
        		        Tuple2<Long, SortedSet<String>>, // T as input
        		        Long, // as output
        		        ArrayList<Long> // as output
        		>() {
        			@Override
        			public Tuple2<Long, ArrayList<Long>> call(Tuple2<Long, SortedSet<String>> element) {
        				ArrayList<Long> longArrayx‬ = new ArrayList<Long>();
                    	for(int i = 0; i < minHashSigLen; i++)
                    		longArrayx‬.add(1L<<42);
        				return new Tuple2<Long, ArrayList<Long>>(element._1, longArrayx‬);
        			}
        });
    
        //System.out.println("minHashPairRDD count: " + minHashPairRDD.count());
        //minHashPairRDD.saveAsTextFile(minHashNumbersFile);

        JavaPairRDD<String, Long> shingleDocIdPairRDD = docIdSortedShingleSetPairRDD
        		.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, SortedSet<String>>, String, Long>() {
			@Override
			public Iterable<Tuple2<String, Long>> call(Tuple2<Long, SortedSet<String>> arg0)
					throws Exception {
				List<Tuple2<String, Long>> result = new ArrayList<Tuple2<String, Long>>();
				for (String elt : arg0._2){
					result.add(new Tuple2<String, Long>(elt, arg0._1));
				}
				return result;
			}
          });
    
        JavaPairRDD<String, Iterable<Long>> shingleDocPairRDDGroupedByKey =  shingleDocIdPairRDD.groupByKey().sortByKey();
        JavaPairRDD<Tuple2<String, Iterable<Long>>, Long> shingleDocIdPairRDDGroupedByKeyIndexed =  shingleDocPairRDDGroupedByKey.zipWithIndex();
        
        JavaPairRDD<Long, Iterable<Long>> shingleIdMappedToDocIdLists =  shingleDocIdPairRDDGroupedByKeyIndexed
        							.mapToPair(new PairFunction<Tuple2<Tuple2<String, Iterable<Long>>, Long>, Long, Iterable<Long>>(){
										@Override
										public Tuple2<Long, Iterable<Long>> call(
												Tuple2<Tuple2<String, Iterable<Long>>, Long> arg0)
												throws Exception {
													long shingleId = arg0._2;
													Iterable<Long> docIdsList = arg0._1._2;											
											return (new Tuple2<Long, Iterable<Long>>(shingleId, docIdsList)) ;
										}
        								
        							}					
        		);

        Long numOfShingles = shingleIdMappedToDocIdLists.count();
        System.out.println("shingleIdMappedToDocIdLists count: " + numOfShingles);
        shingleIdMappedToDocIdLists.saveAsTextFile(shingleIdMappedToDocIdListsFile);
        
        HashFunctionUtility hashUtil = new HashFunctionUtility(numOfShingles);
        hashUtil.SelectRandomPrimes(minHashSigLen);
        final Broadcast<HashFunctionUtility> broadCastedHashUtil = sc.broadcast(hashUtil);
        
        //int minHashIndex=0;
        
        JavaPairRDD<Long,ArrayList<Long>> docIdMinHashValuePairs = shingleIdMappedToDocIdLists
        			.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long,ArrayList<Long>>() {
			@Override
			public Iterable<Tuple2<Long, ArrayList<Long>>> call(Tuple2<Long, Iterable<Long>> arg0)
					throws Exception {
				ArrayList<Tuple2<Long,ArrayList<Long>>> docIdHashValuesPair = new ArrayList<Tuple2<Long,ArrayList<Long>>>();
				for (Long docId: arg0._2){
					ArrayList<Long> hashValues = new ArrayList<Long>();
					for(int i = 0; i < minHashSigLen; i++){
						Long primeForHash = broadCastedHashUtil.getValue().selectedRandomPrimes.get(i);
						hashValues.add(broadCastedHashUtil.getValue().FindHashValue(arg0._1, primeForHash));
					}
					//Long hashVal = broadCastedHashUtil.getValue().FindHashValue(arg0._1, primeForHash);
					docIdHashValuesPair.add(new Tuple2<Long,ArrayList<Long>>(docId, hashValues));
				}
				return docIdHashValuesPair;
			}
          }).reduceByKey(new Function2<ArrayList<Long>,ArrayList<Long>,ArrayList<Long>>(){
        	  @Override
        	  public ArrayList<Long> call(ArrayList<Long> l1, ArrayList<Long> l2){
        		  ArrayList<Long> minHashValues = new ArrayList<Long>();
        		  for(int i = 0; i < minHashSigLen; i++){
        			  minHashValues.add(Math.min(l1.get(i), l2.get(i)));
        		  }
        		  return minHashValues;
        	  }
          }).sortByKey();
        
        Long docIdMinHashValuePairsCount = docIdMinHashValuePairs.count();
        System.out.println("docIdMinHashValuePairsCount: " + docIdMinHashValuePairsCount);
        docIdMinHashValuePairs.saveAsTextFile(docIdMinHashValuePairsFile);
          
        int numOfLSHBlocks = minHashSigLen/lshBlockSize;
        for(int ll = 0; ll < numOfLSHBlocks; ll++){
        	String path = candiateDocSetsFile + Integer.toString(ll);
        	final int loopvalue = ll;  //Need loopValue as final to be able to use it in the map function below
            JavaPairRDD<Long, Iterable<Long>> LSHBuckets = docIdMinHashValuePairs.mapToPair(
            		new PairFunction<
            		        Tuple2<Long, ArrayList<Long>>, // T as input
            		        Long, // as output hash value
            		        Long // as output doc Id
            		>() {
            			@Override
            			public Tuple2<Long, Long> call(Tuple2<Long, ArrayList<Long>> element) {
            				Long sum=0L;
                        	for(int i = 0; i < lshBlockSize; i++){
                        		sum += element._2.get(loopvalue*lshBlockSize + i);
                        	}
                        	
            				return new Tuple2<Long, Long>(sum%lshBucketsSize, element._1);
            			}
            }).groupByKey();
            System.out.println("Saving candidate doc sets that appear to be similar in file: " + path);
            LSHBuckets.saveAsTextFile(path);
            
        }
        //System.out.println("shingleIdDocIdPairRDDGroupedByKey count: " + shingleIdDocIdPairRDDGroupedByKey.count());
        //shingleIdDocIdPairRDDGroupedByKey.saveAsTextFile(shingleIdDocIdPairRDDGroupedByKeyFile);
    
      
        
        //System.out.println("minHashPairRDD count: " + minHashPairRDD.count());
        //minHashPairRDD.saveAsTextFile(minHashNumbersFile);
        
        
    }
}
