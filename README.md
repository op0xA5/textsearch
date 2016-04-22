# textsearch
基于纯文本的索引工具

```
Usage: textsearch [-d directory] [-i index file] search
       textsearch -m [-d directory] [-i index file] pattern

```

## 索引规则

- 基于有序数组，查询效率为```log(n)```
- 对单一目录里所有文件统一制作索引文件
- 对文本文件以行为单位进行扫描，使用正则表达式 (pattern参数) 进行关键词提取
- 已经制作好索引的原始文件不得进行任何修改，否则需要重新制作索引

## TODO

- 可对单一文件制作索引
- 制作索引或进行查询时支持目录递归
- 查询时支持不区分大小写
- 测试正则表达式功能
- 支持正则表达式匹配模式
