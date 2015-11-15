package org.kumar.SalonTableTalk;

import java.io.Serializable;
import java.util.ArrayList;

public class HashFunctionUtility implements Serializable {
    private static final long serialVersionUID = -13223239926230329L;

	public Long numOfShingles = 0L;
	public ArrayList<Long> primeNumbers = new ArrayList<Long>();
	public ArrayList<Long> selectedRandomPrimes;

	public boolean isPrime(long n){
		boolean prime=true;
		double cutoff = Math.sqrt(n)+1;
		for(Long p: primeNumbers){
			if(n%p == 0){
				prime = false;
				break;
			}
			if (p > cutoff)
				break;
		}
		return prime;
	}
	
	public void GeneratePrimeNumbers(){
		primeNumbers.add(2L);
		for (long n = 3; n < 1000000L; n++){
			if (isPrime(n)){
				primeNumbers.add(n);
				//System.out.print(n + "  ");
			}
		}
		
		System.out.println("Generated " + primeNumbers.size() + " primes");
		//System.out.println(primeNumbers.toString());
	}

	public void Hash1toN(long N, long primeNumber){
		Long val=0L;

		long sum=0;
		for(long l = 0; l < N; l++){
			val = ((l+1)*primeNumber)%(N+1);
			val = val*primeNumber%(N+1);
			sum += val;
			if ((l+1) % 10000 == 0){
				System.out.println("At: " + l + " Sum of prev 10000 hash values: " + sum);
				sum=0;
			}
		}
		System.out.println("Hash 1 to N Completed");
	}

	// The 20,000th prime is 224743 and its square of this is sufficiently large for our purpose of finding P^2%N.
	// So, let us choose primes greater than the 20000th prime. 
	public void SelectRandomPrimes(int numOfPrimesToBeSelected){
		selectedRandomPrimes = new ArrayList<Long>();
		int indexOfPrime;
		for (int i = 0; i < numOfPrimesToBeSelected; i++){
			indexOfPrime = 20000 + (int)(Math.random()*50000);
			selectedRandomPrimes.add(primeNumbers.get(indexOfPrime));
		}		
	}
	
	public Long FindHashValue(Long shingleId, Long primeNumber){
		Long val = ((shingleId+1)*primeNumber)%(numOfShingles+1);
		val = ((val)*primeNumber)%(numOfShingles+1);
		return (val -1);
	}
	
	
	public HashFunctionUtility(Long N) {
		numOfShingles = N;
		GeneratePrimeNumbers();
	}

}
