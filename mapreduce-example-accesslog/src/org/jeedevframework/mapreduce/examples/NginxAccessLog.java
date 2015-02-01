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
package org.jeedevframework.mapreduce.examples;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.jeedevframework.mapreduce.utils.LogAnaHelper;

/**
 * 按秒统计每个IP访问次数比较高的记录
 * @author huligong
 * */
public class NginxAccessLog {
	
	
	/**
	 *nginx access_log内容格式  39.182.130.55 - - [22/Dec/2014:00:00:00 +0800] "POST /api/abc.do HTTP/1.1" 200 29 "-" "-"
	 * 
	 * */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static SimpleDateFormat SDF_PARSE = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
		private final static SimpleDateFormat SDF_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		private final static IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(Object key, Text value,Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 将输入的纯文本文件的行数据转化成String
			String line = value.toString();
			
			//正则表达式，处理nginx access_log行数据，提取所需数据
			String regex = "^(?<ip>\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})\\s-\\s(.*)\\s\\[(?<date>.*)\\]\\s\"(?<method>POST|GET)\\s(?<request>.*)\"\\s(?<status>\\d{3,})\\s(?<size>\\d+)\\s\"(.*)$";//
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				String ip = LogAnaHelper.ipComplete(matcher.group("ip"));
				String dateString = matcher.group("date");
				String requestMethod = matcher.group("method");
				String request = matcher.group("request");
				String url = request.split("\\s")[0];
				String status = matcher.group("status");
				String size = matcher.group("size");

				
				//不统计‘/api/test.do’ 地址访问请求情况
				if("/api/test.do".equals(url)){
					return;
				}
				
				String time = null;
				try {
					time = SDF_FORMAT.format(SDF_PARSE.parse(dateString));
				} catch (ParseException e) {
					e.printStackTrace();
				}

				if (null == time) {
					time = dateString;
				}

				//String info = time + "|" + ip + "|" + requestMethod + "|" + url + "|" + status + "|" + size;
				//System.out.println(info);

				// key --> 2014-12-21 00:00:00|039.182.130.055|/api/abc.do
				String keyString = time + "|" + ip + "|" + url;
				Text keyText = new Text(keyString);
				context.write(keyText, one);
			} else {
				System.out.println("no match:" + line);
			}
		}
	}
  
	public static class IntSumCombinerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
  
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			//仅输出大于5次的记录
			if(sum>=5){
				result.set(sum);
				context.write(key, result);				
			}
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
      System.err.println("Usage: nginx accesslog <in> [<in>...] <out>");
      System.exit(2);
    }
    
    Job job = Job.getInstance(conf, "nginx accesslog ");
    job.setJarByClass(NginxAccessLog.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombinerReducer.class);
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
