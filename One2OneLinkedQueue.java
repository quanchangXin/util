package com.seven.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 利用volatile 语义，不需要锁和CAS操作（CAS操作可能会锁住总线）
 * 仅适用于一个生产者一个消费者的情况
 * @author seven
 *
 * @param <T>
 */
public class One2OneLinkedQueue<T> {
	static class Node<T> {
		T value;
		Node<T> next = null;

		Node(T value) {
			this.value = value;
		}
	}

	private volatile Node<T> refHead, refTail;

	public One2OneLinkedQueue() {
		Node<T> dummy = new Node<T>(null);
		refHead = dummy;
		refTail = dummy;
	}

	public boolean enQueue(T value) {
		if (value == null)
			throw new NullPointerException();
		
		Node<T> node = new Node<T>(value);
		refTail.next = node;
		refTail = node;
		
		return true;
	}

	public T deQueue() {
		if (refHead == refTail) {
			return null;
		}

		T value = refHead.next.value;
		//gc
		refHead.next.value = null;
		refHead = refHead.next;
		return value;
	}

	public static String genElement() {
		Random random = new Random();
		return "" + (char) ('a' + random.nextInt(26));
	}

	public static void main(String[] args) throws InterruptedException {

		final One2OneLinkedQueue<String> queue = new One2OneLinkedQueue<String>();
		final List<String> enList = new ArrayList<String>();
		final List<String> deList = new ArrayList<String>();
		Thread thread1 = new Thread(new Runnable() {

			@Override
			public void run() {
				Random random = new Random();
				String elment;

				for (;;) {
					elment = genElement();
					queue.enQueue(elment);
					enList.add(elment);
					System.out.println("enQueue " + elment);

					try {
						Thread.sleep(100 * random.nextInt(10));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						break;
					}
				}
			}

		});

		Thread thread2 = new Thread(new Runnable() {

			@Override
			public void run() {
				String elment;

				for (int i = 0; i < 1000000; i++) {
					elment = queue.deQueue();
					if(elment != null){
						deList.add(elment);
					}
					
					System.out.println("deQueue " + elment);

					// try {
					// Thread.sleep(100*random.nextInt(10));
					// } catch (InterruptedException e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// break;
					// }
				}

			}

		});

		thread1.start();
		thread2.start();

		Thread.sleep(10000);
		thread1.interrupt();
		thread2.interrupt();

		System.out.println("enQueue elements");
		for (String e : enList) {
			System.out.print(e + " ");
		}
		System.out.println();

		System.out.println("deQueue elements");
		for (String e : deList) {
			System.out.print(e + " ");
		}
		System.out.println();
	}
}
