package cmsc433.p5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final int          TWEET_SCORE   = 1;
	public static final int          RETWEET_SCORE = 3;
	public static final int          MENTION_SCORE = 1;
	public static final int			 PAIR_SCORE = 1;

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	public static class TweetMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(value.toString());

			// TODO: Your code goes here
			switch(trendingOn) {
			case USER:
				
				for (String user : tweet.getMentionedUsers()) {
					context.write(new Text(user), new IntWritable(MENTION_SCORE));
				}
				
				String retweetedUser = tweet.getRetweetedUser();
				if (retweetedUser != null) {
					context.write(new Text(retweetedUser), new IntWritable(RETWEET_SCORE));
				}
				
				
				context.write(new Text(tweet.getUserScreenName().toString()), new IntWritable(TWEET_SCORE));
				
				
				break;
				
				
			case TWEET:
				
				
				Long retweeted = tweet.getRetweetedTweet();
				
				if (retweeted != null) {
					context.write(new Text(retweeted.toString()), new IntWritable(RETWEET_SCORE));
					
				}
				context.write(new Text(tweet.getId().toString()), new IntWritable(TWEET_SCORE));
				
				
				break;
			case HASHTAG:
				for (String hashtag : tweet.getHashtags()) {
					String finalHashTag = removeHashTag(hashtag);
					context.write(new Text(finalHashTag), new IntWritable(1));
				}
				
				break;
				
			case HASHTAG_PAIR:
				
				
				if (tweet.getHashtags().size() >= 2) {
					ArrayList<String> pairs = new ArrayList<String>();
					List<String> hashtags = new ArrayList<String>();
					for (String tag : tweet.getHashtags()) {
						hashtags.add(removeHashTag(tag));	
					}
					
					
					
					
					for (int i = 0; i < hashtags.size(); ++i) {
					     for (int j = i + 1; j < hashtags.size(); ++j) {
					    	 String[] p = new String[]{hashtags.get(i), hashtags.get(j)};  
					    	 String formattedOutput = formatOutput(Arrays.asList(p));
					    	 if (!pairs.contains(formattedOutput)) {
									pairs.add(formattedOutput);
							 }
					     }
					 }

					for (String hashtagPair : pairs) {
						context.write(new Text(hashtagPair), new IntWritable(PAIR_SCORE));
					}
				} 
				
				break;
	
			}

		}

	}
	
	private static String formatOutput(List<String> hashtags) {
		// TODO Auto-generated method stub
		String pair;
		
		
		
		if (hashtags.get(0).compareTo(hashtags.get(1)) < 0) {
			pair = "(" + hashtags.get(1) + "," + hashtags.get(0) + ")";
		} else {
			pair = "(" + hashtags.get(0) + "," + hashtags.get(1) + ")";
		}
		
		
		return pair;
	}
	
	
	private static String removeHashTag(String hashTag) {
		return hashTag.replace("#", "");
	}
	

	public static class PopularityReducer
	extends Reducer<Text, IntWritable, Text ,IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int score = 0;
			for (IntWritable value : values) {
				score += value.get();
			}
				
			context.write(new Text(key.toString()), new IntWritable(score));
			
		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;

		job.setJarByClass(TweetPopularityMR.class);

		// TODO: Set up map-reduce...
		job.setJobName("Scores");
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(input));
		TextOutputFormat.setOutputPath(job, new Path(output));


		return job.waitForCompletion(true);
	}
}
