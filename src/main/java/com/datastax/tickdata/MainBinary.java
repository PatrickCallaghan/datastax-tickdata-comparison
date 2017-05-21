package com.datastax.tickdata;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.tickdata.engine.TickGenerator;
import com.datastax.tickdata.model.TickData;
import com.datastax.timeseries.model.TimeSeries;

public class MainBinary {
	private static Logger logger = LoggerFactory.getLogger(MainBinary.class);
	private AtomicLong binaryTotal = new AtomicLong(0);
	private AtomicLong tickTotal = new AtomicLong(0);

	private String pattern = "#,###,###.###";
	private DecimalFormat decimalFormat = new DecimalFormat(pattern);

	public MainBinary() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "8");
		String noOfDaysStr = PropertyHelper.getProperty("noOfDays", "1");

		int noOfDays = Integer.parseInt(noOfDaysStr);
		DateTime startTime = new DateTime().minusDays(noOfDays);

		logger.info("StartTime : " + startTime);

		TickDataBinaryDao binaryDao = new TickDataBinaryDao(contactPointsStr.split(","));

		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		// Create shared queue
		BlockingQueue<TimeSeries> binaryQueue = new ArrayBlockingQueue<TimeSeries>(100);

		// Executor for Threads
		ExecutorService binaryExecutor = Executors.newFixedThreadPool(noOfThreads);

		Timer timer = new Timer();
		timer.start();

		for (int i = 0; i < noOfThreads; i++) {

			binaryExecutor.execute(new TimeSeriesWriter(binaryDao, binaryQueue));
		}

		// Load the symbols
		DataLoader dataLoader = new DataLoader();
		List<String> exchangeSymbols = dataLoader.getExchangeData();

		logger.info("No of symbols : " + exchangeSymbols.size());

		// Start the tick generator
		TickGenerator tickGenerator = new TickGenerator(exchangeSymbols, startTime);

		while (tickGenerator.hasNext()) {
			TimeSeries next = tickGenerator.next();

			try {
				binaryQueue.put(next);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		timer.end();

		while (!binaryQueue.isEmpty()) {
			sleep(1);
		}

		logger.info("Data Loading ("
				+ decimalFormat.format(tickGenerator.getCount())
				+ " ticks) for binary took "
				+ binaryTotal.get()
				+ "ms ("
				+ decimalFormat.format(new Double(tickGenerator.getCount() * 1000)
						/ (new Double(binaryTotal.get()).doubleValue())) + " a sec)");

		System.exit(0);
	}

	class TimeSeriesWriter implements Runnable {

		private TickDataBinaryDao binaryDao;
		private BlockingQueue<TimeSeries> binaryQueue;

		public TimeSeriesWriter(TickDataBinaryDao binaryDao, BlockingQueue<TimeSeries> binaryQueue) {
			logger.info("Created binary writer");

			this.binaryDao = binaryDao;
			this.binaryQueue = binaryQueue;
		}

		@Override
		public void run() {
			TimeSeries timeSeriesBinary;

			while (true) {
				timeSeriesBinary = binaryQueue.poll();

				if (timeSeriesBinary != null) {
					try {
						Timer binary = new Timer();
						this.binaryDao.insertTimeSeries(timeSeriesBinary);
						binary.end();
						binaryTotal.addAndGet(binary.getTimeTakenMillis());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MainBinary();
	}
}
