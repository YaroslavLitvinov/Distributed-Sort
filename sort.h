/*
 * sort.h
 *
 *  Created on: 17.03.2012
 *      Author: yaroslav
 */

#ifndef SORT_H_
#define SORT_H_

#include <stdint.h> //uint32_t
#include <stddef.h> //size_t


typedef uint32_t* BigArrayPtr;
typedef uint32_t  BigArrayItem;
typedef struct histogram_item_t *HistogramArrayPtr;
typedef struct histogram_item_t HistogramArrayItem;

struct histogram_item_t
{
	int item_index;
	int last_item_index;
	BigArrayItem item;
};


void print_histogram( const HistogramArrayPtr histogram, size_t len );

HistogramArrayPtr
alloc_histogram_array_get_len(
		const BigArrayPtr array, int offset, const int array_len, int step, int *histogram_len );
int run_sort( BigArrayPtr *unsorted, BigArrayPtr *sorted, int sortlen );
BigArrayPtr alloc_array_fill_random( int array_len );
BigArrayPtr alloc_merge_sort( BigArrayPtr array, int array_len );
BigArrayPtr merge( BigArrayPtr left_array, int left_array_len,
		BigArrayPtr right_array, int right_array_len );
void print_array(const char* text, BigArrayPtr array, int len);
int test_sort_result( BigArrayPtr unsorted, BigArrayPtr sorted, int len );
uint32_t array_crc( BigArrayPtr array, int len );



#endif /* SORT_H_ */
