# reborn

此repo是以下问题的一个可能的解决方案: [Question](
https://docs.google.com/document/d/1WWaE1eTXmFT9VCopVEV5rvxfyKIhYHck9qC0V3m2iGA/edit?usp=sharing)

#算法

0. 此repo仅为验证想法(没有更多保证 ;-))
1. 因为 update = 先delete再add,所以流中的所有操作可转化为delete与add两个操作
2. 从流中连续读出数据构成段:S直到某一个update/delete操作的下一个操作为add为止(这样可以达到分段数最少)
3. 将S中的所有update操作用同logID的delete操作与add操作替换(此操作可以并发)
4. 从段S尾部向头部查找，将每一个uid的logID最大UserRecord择出，放入重建队列(此操作可以并发)
5. 因为operation log能成功输出，所以每个uid的logID最大的UserRecord即是他的最终状态，所以不用关注nickname与mobile是不是在中间状态时
   与其他uid有冲突,因为分段及并发操作的原因，所以某uid的delete操作与其转化过后的add操作，可能并不在一个段中，所以在最后的重建队列中，需要
   通过某种方式(此实现中通过uid与最大logID的 operation log的映射来实现)找到某uid对应的最大logID的操作，然后判断
   他的最后操作是什么，如果是delete,则忽略之，如果是add则作为最终的重建数据
6. 重复2-5直到完成
