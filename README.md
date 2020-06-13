# Twitter-Analyzer

Running the program 
*Trending param:
  * The key to be used for themap-reduce.
  * Choices: user, tweet, hashtag or hashtag_pair.
  * This decides which entity we will be ranking.
 
*cutoff:
   *The minimum score required for an entry to be included in the final output of the program.
*in:
  *This is a path to the input CSV (Comma-Separated Value) file containing the tweets on which to perform the map-reduce.
  *Each line in the file represents a tweet that we conveniently organized for you.
*out:
  *This is a path to a directory where files will be written at the end of map-reduce.
  *Thedirectorymust not exist when the program is run, or else Hadoop will throwan error.As an example, typing the command
  java -jar twitter.jar user 500 in.csv out should result in ranking the users with at least a score of 500.
