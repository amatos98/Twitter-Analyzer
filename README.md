# Twitter-Analyzer
## This project was implemented in Hadoop to process and manipulate a large and unstructured dataset consisted of Twitter posts to rank the popularity of users, tweets, hashtags, and pairs of hashtags. 

### Running the program

* Trending param:
  * The key to be used for themap-reduce.
  * Choices: user, tweet, hashtag or hashtag_pair.
  * This decides which entity we will be ranking.
 
* cutoff:
   * The minimum score required for an entry to be included in the final output of the program.
* in:
  * This is a path to the input CSV (Comma-Separated Value) file containing the tweets on which to perform the map-reduce.
 
* out:
  * This is a path to a directory where files will be written at the end of map-reduce.
  * The directory must not exist when the program is run, or else Hadoop will throwan error. As an example, typing the command
  
  java -jar twitter.jar user 500 in.csv out 
  
  
  should result in ranking the users with at least a score of 500.
