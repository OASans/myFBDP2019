package com.mapreduce.homework4_2_6.Utils;

import java.util.List;

public interface Distance<T> {
	double getDistance(List<T> a,List<T> b) throws Exception;
}
