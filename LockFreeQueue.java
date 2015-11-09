package com.wuzhi.concurrent;

import java.util.concurrent.atomic.AtomicReference;

/**
 * code from codereview.stackexchange.com
 * 
 * use CAS operation and volatile semantic
 * in jvm memory model when read a volatile variable from main memory to working memory there are three operation read load and use 
 * each depends on before operation read must be executed before load and load must be executed before use.
 * when write a volatile variable to main memory from working memory assign store write executed sequentially 
 * volatile variable have memory barrier semantic
 * 
 * 
 * jvm memory model docs
 * docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.4.2
 * 
 * barrier
 * 
 * from Doug Lea 
 *   Anything that was visible to thread A when it writes to volatile field f becomes visible to thread B when it reads f.
 *   Note that it is important for both threads to access the same volatile variable in order to properly set up the happens-before relationship. It is not the case that everything visible to thread A when it writes volatile field f becomes visible to thread B after it reads volatile field g.
 * 
 * gee.cs.oswego.edu/dl/jmm/cookbook.html
 * @param <T>
 */
public class LockFreeQueue<T> {
   private static class Node<E> {
	   // there is a StoreStore Barrier  between Normal Store  and Volatile Store 
	   // StoreStore Barrier
	   // The sequence: Store1; StoreStore; Store2
	   // ensures that Store1's data are visible to other processors (i.e., flushed to memory) before the data associated with Store2 and all subsequent store instructions. In general, StoreStore barriers are needed on processors that do not otherwise guarantee strict ordering of flushes from write buffers and/or caches to other processors or main memory. 
       E value;//there is no need to use volatile
       volatile Node<E> next = null;

       Node(E value) {
           this.value = value;
       }
   }

   private AtomicReference<Node<T>> refHead, refTail;

   public LockFreeQueue() {
       Node<T> dummy = new Node<T>(null);
       refHead = new AtomicReference<Node<T>>(dummy);
       refTail = new AtomicReference<Node<T>>(dummy);
   }

   public void enQueue(T value) {
       if (value == null)
           throw new NullPointerException();

       Node<T> node = new Node<T>(value);
       //atomic set refTail ref to the current node and return prevous node
       Node<T> prefTail = refTail.getAndSet(node);
       prefTail.next = node;
   }

   public T deQueue() {
       Node<T> head, next;

       do {
           head = refHead.get();
           next = head.next;
           if (next == null) {
               return null;
           }
       } while (!refHead.compareAndSet(head, next));

       T value = next.value;
       //gc
       next.value = null;

       return value;
   }

   public static void main(String[] args) {

   }
}
