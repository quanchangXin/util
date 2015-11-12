package com.seven.concurrent;

/**
 * 利用volatile 语义，不需要锁和CAS操作（CAS操作可能会锁住总线）
 * 仅适用于一个生产者一个消费者的情况
 * @author seven
 *
 */
public class One2OneCircleQueue<T> {
	private int length;
	private volatile int i = 0;
	private volatile int j = 0;
	private Object[] array;

	public One2OneCircleQueue(int length){
		this.length = length;
		array = new Object[this.length];
	}
	
	public boolean enQueue(T value){
		//queue is full
		if(i == (j + 1) % length)
			return false;
		
		//如果queue不是满的，那么可以加入元素(只有一个写线程)
		array[j] = value;
		j = (j + 1) % length;
		return true;
	}
	
	public T deQueue(){
		//queue is empty
		if(i == j)
			return null;
		
		Object value = array[i];
		i = (i + 1) % length;
		
		return (T) value;
	}
}
