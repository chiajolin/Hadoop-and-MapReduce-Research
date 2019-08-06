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
