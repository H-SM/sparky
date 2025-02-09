Following is the complete output for the script `sparky.py`: 

```python
=== Creating RDDs ===
RDD from list: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]                                  

RDD from text file: ['Hello This is HSM.', 'This is a sample spark RDD over text file.', 'Sparky is out to go wild and zap everyone.']

RDD from CSV: ['id,name,mob', '261723,Harman,8792327121', '223173,Sid,87923228201', '261281,Saksham,8792317121']

=== Demonstrating Transformations ===
Squared numbers: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

Even numbers: [2, 4, 6, 8, 10]

Words from text: ['Hello', 'This', 'is', 'HSM.', 'This', 'is', 'a', 'sample', 'spark', 'RDD', 'over', 'text', 'file.', 'Sparky', 'is', 'out', 'to', 'go', 'wild', 'and', 'zap', 'everyone.']

=== Demonstrating Actions ===
Collected elements: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

Total elements: 10

Sum of elements: 55

First 5 elements: [1, 2, 3, 4, 5]
```