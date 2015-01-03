/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	
/**
 * Map过程需要继承org.apache.hadoop.mapreduce包中Mapper类，并重写其map方法。
 * 通过在map方法中添加两句把key值和value值输出到控制台的代码，可以发现map方法中value值存储的是文本文件中的一行（以回车符为行结束标记），
 * 而key值为该行的首字母相对于文本文件的首地址的偏移量。然后StringTokenizer类将每一行拆分成为一个个的单词，
 * 并将<word,1>作为map方法的结果输出，其余的工作都交有MapReduce框架处理。
 * */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());//构造一个用来解析str的StringTokenizer对象,如果不指定分隔字符，默认的是：”\t\n\r\f”
      while (itr.hasMoreTokens()) {//hasMoreTokens 是否还有分隔符
        word.set(itr.nextToken());// nextToken 从当前位置到下一个分隔符的字符串
        context.write(word, one);
      }
    }
  }
  
  /**
   * Reduce过程需要继承org.apache.hadoop.mapreduce包中Reducer类，并重写其reduce方法。
   * Map过程输出<key,values>中key为单个单词，而values是对应单词的计数值所组成的列表，
   * Map的输出就是Reduce的输入，所以reduce方法只要遍历values并求和，即可得到某个单词的总次数。
   * 
   * */
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  
  
  /**
   * 在MapReduce中，由Job对象负责管理和运行一个计算任务，并通过Job的一些方法对任务的参数进行相关的设置。
   * 此处设置了使用TokenizerMapper完成Map过程中的处理和使用IntSumReducer完成Combine和Reduce过程中的处理。
   * 还设置了Map过程和Reduce过程的输出类型：key的类型为Text，value的类型为IntWritable。
   * 任务的输出和输入路径则由命令行参数指定，并由FileInputFormat和FileOutputFormat分别设定。
   * 完成相应任务的参数设定后，即可调用job.waitForCompletion()方法执行任务。
   * */
  public static void main(String[] args) throws Exception {
	
	 // args = new String[2];
	 // args[0] = "hdfs://ubuntu-V01:9000/user/hadoop/input";
	 // args[1] = "hdfs://ubuntu-V01:9000/user/hadoop/output";
	  
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
