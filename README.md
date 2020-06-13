# Twitter-Analyzer

Running the program 
•Trending param:
  o The key to be used for themap-reduce.
  o Choices: user, tweet, hashtag or hashtag_pair.
  o This decides which entity we will be ranking.
 
•cutoff:
   oThe minimum score required for an entry to be included in the final output of the program.
•in:
  oThis is a path to the input CSV (Comma-Separated Value) file containing the tweets on which to perform the map-reduce.
  oEach line in the file represents a tweet that we conveniently organized for you.
•out:oThis is a path to a directory where files will be written at the end of map-reduce.
  oThedirectorymust not exist when the program is run, or else Hadoop will throwan error.As an example, typing the commandjava -jar twitter.jar user 500 in.csv outshould result inranking the users with at least a scoreof 500.
