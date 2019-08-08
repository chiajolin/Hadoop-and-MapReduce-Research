# Hadoop-and-MapReduce-Research

PATH
1.	S3 Bucket: s3://chiajolin/
2.	Output Path:
  a.	WordCount: s3://chiajolin/WordCount/output
  b.	Task A on SmallChessInput: s3://chiajolin/WinnerCount/SmallOutput
  c.	Task A on VeryLargeChessInput: s3://chiajolin/WinnerCount/LargeOutput
  d.	Task B on SmallChessInput: s3://chiajolin/Opponent/SmallOutput
  e.	Task B on VeryLargeChessInput: s3://chiajolin/Opponent/LargeOutput
  f.	Task C on SmallChessInput: s3://chiajolin/Sort/SmallOutput
  g.	Task A on VeryLargeChessInput: s3://chiajolin/Sort/LargeOutput

For Task A(WordCount), values output  
1.	On SmallChessInput  
    BLACK	9559	0.3797926  
    DRAW	4582	0.18204935  
    WHITE	11028	0.43815807  

2.	On VeryLargeChessInput  
    BLACK	13187704	0.46653244  
    DRAW	1074045	0.03799576  
    WHITE	14005748	0.49547184  

Code Description
Part A (WonCount.java):  
  1. Class TokenizerMapper: Read data and use tag [result] to count total number of games.This class is just for counting,        so I don’t implement its reducer.  
  2. Class FinalMapper: Read and parse data. If the result is BLACK, output (BLACK, 1). Similar output for WHITE and DRAW.  
  3. Class MyCombiner: Implement combiner as an optimization. The combiner will summarize the mapper output records and pass      its output to reducer if possible. The output type of MyCombiner is the same as the output type of FinalMapper.  
  4. Class FinalReducer: Output sum and percentage for games won by BLACK, WHITE or DRAW.

Part B (Opponents.java):  
  1. Class CustomWritable: This is one of my custom writable. I use CustomWritable to store my mapper output value. The          value type is (gameNumber, conWin, conLose, conWinTag, conLoseTag). gameNumber stores the number of opponents. 
     conWin stores the time player won against a higher-rated (Elo score) opponent. conLose stores the time player lost 
     against a lower rated opponent. conWinTag is the tag I use to check if the player never played a higher rated opponent      as the color. conLoseTag is the tag I use to check if the player never played a lower rated opponent as the color.
  2. Class OutputWritable: This is one of my custom writable. I use OutputWritable to store my reducer output value.
  3. Class TokenizerMapper: Read data and use tag [White], [Black], [WhiteElo], [BlackElo], [Result] to get the information      I want. First, I use BlackElo and WhiteElo to set the conWinTag and conLoseTag. If BlackElo > WhiteElo, set Black value      to (0,0,0,-1,1) and set White value to (0,0,0,1,-1). Similar setting for WhiteElo > BlackElo. Second, I use the 
     result to set gameNumber, conWin, and conLose. 
  4. Class MyCombiner: Implement combiner as an optimization. The combiner will summarize the mapper output records and pass      its output to reducer if possible. The output type of MyCombiner is the same as the output type of TokenizerMapper. The      combiner will sum the gameNumber, conWin, and conLose. If one of the conWinTag value from the mapper equals 1, set          conWinTag equals 1 which means the final output should not be na. Same operation when setting conLoseTag.
  5. Class IntSumReducer: Reducer will do the same operation as combiner. And After that, reducer will compute the        
     percentage and check if conWinTag or conLoseTag not equal to 1 . If conWinTag not equals to 1, set the conWin to na. If      conLoseTag not equals to 1, set the conLoseTag to na. Otherwise, output the value OutputWritable (the number of              opponents they played, the percentage of time they won against a higher-rated opponent, lost against a lower rated          opponent).
