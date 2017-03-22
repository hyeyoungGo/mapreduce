package com.bit2017.mapreduce.test.pq;

import java.util.PriorityQueue;

public class TestMain {
	
	public static void main(String[] args) {
		PriorityQueue<String> pq = new PriorityQueue<String>( 10, new StringComparator() );
		
		pq.add( "hello" );
		pq.add( "Hello World" );
		pq.add( "hi" );
		pq.add( "abcdefg" );
		
		while( pq.isEmpty() == false ) {
			String s = pq.remove();
			System.out.println( s );
		}
		
	}
}
