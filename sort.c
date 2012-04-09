/*
 * sort.c
 *
 *  Created on: 16.03.2012
 *      Author: yaroslav
 *      Merge sorting recursive algorithm implementation.
 */


#include "sort.h"
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <sys/types.h> //pid_t
#include <unistd.h> //getpid()


void copy_array( BigArrayPtr dst_array, const BigArrayPtr src_array, int array_len );
BigArrayPtr alloc_copy_array( const BigArrayPtr array, int array_len );



void
print_histogram( const HistogramArrayPtr histogram, size_t len ){
	for ( int j=0; j < len && j < 20; j++ ){
		printf( "[%d]=[%d, %d), ",
				histogram[j].item, histogram[j].item_index, (int)histogram[j].last_item_index );
	}
	fflush(0);
}


HistogramArrayPtr
alloc_histogram_array_get_len(
		const BigArrayPtr array, int offset, const int array_len, int step, int *histogram_len ){
	*histogram_len = array_len/step;
	if ( *histogram_len * step < array_len )
		++*histogram_len;
	int h = 0, i = 0;
	HistogramArrayItem histogram_item;
	HistogramArrayPtr histogram_array = malloc( sizeof(HistogramArrayItem) * *histogram_len );
	for( i=0, h=0; i < array_len && h < *histogram_len; i+=step ){
		histogram_item.item_index = i+offset;
		histogram_item.item = array[i+offset];
		histogram_item.last_item_index = histogram_item.item_index + step -1;
		histogram_array[h++] = histogram_item;
	}
	//add histogram item with histogram_step < step
	if ( histogram_item.item_index < array_len-1 && h < *histogram_len ){
		histogram_item.item_index = offset+ array_len-1;
		histogram_item.item = array[offset+array_len-1];
		histogram_item.last_item_index = histogram_item.item_index + array_len-1-i-1;
		histogram_array[h] = histogram_item;
	}
	return histogram_array;
}

BigArrayPtr
alloc_array_fill_random( int array_len ){
	BigArrayPtr unsorted_array = malloc( sizeof(BigArrayItem)*array_len );

	pid_t pid = getpid();
	//fill array by random numbers
	srand((time_t)pid );
	for (int i=0; i<array_len; i++){
		unsorted_array[i]=rand();
	}
	return unsorted_array;
}


BigArrayPtr
alloc_merge_sort( const BigArrayPtr array, int array_len ){
	if ( array_len <= 1 )
		return alloc_copy_array( array, array_len );

	int middle = array_len/2;
	BigArrayPtr left = alloc_merge_sort( array, middle );
	BigArrayPtr right = alloc_merge_sort( array+middle, array_len-middle );

	BigArrayPtr result = merge( left, middle, right, array_len-middle );
	free(left);
	free(right);
	return result;
}

/**@param global_array_index is used to save result to correct place*/
BigArrayPtr
merge(
		const BigArrayPtr left_array, int left_array_len,
		const BigArrayPtr right_array, int right_array_len ){
	BigArrayPtr larray = left_array;
	BigArrayPtr rarray = right_array;
	BigArrayPtr result = malloc( sizeof(BigArrayItem) *(left_array_len+right_array_len));
	int current_result_index = 0;
	while ( left_array_len > 0 && right_array_len > 0 ){
		if ( larray[0] <= rarray[0]  ){
			result[current_result_index++] = larray[0];
			++larray;
			--left_array_len;
		}
		else{
			result[current_result_index++] = rarray[0];
			++rarray;
			--right_array_len;
		}
	}

	//if merge arrays not empty then it can hold last item
	if ( left_array_len > 0 ){
		copy_array( result+current_result_index, larray, left_array_len );
	}
	if ( right_array_len > 0 ){
		copy_array( result+current_result_index, rarray, right_array_len );
	}

	return result;
}


void copy_array( BigArrayPtr dst_array, const BigArrayPtr src_array, int array_len ){
	for ( int i=0; i < array_len; i++ )
		dst_array[i] = src_array[i];
}


BigArrayPtr alloc_copy_array( const BigArrayPtr array, int array_len ){
	BigArrayPtr newarray = malloc( sizeof(BigArrayItem)*array_len );
	for ( int i=0; i < array_len; i++ )
		newarray[i] = array[i];
	return newarray;
}

void print_array(const char* text, BigArrayPtr array, int len){
	puts(text);
	if ( len > 100 ) len = 100;
	for (int j=0; j<len; j++){
		if ( !j ) printf( "%d", array[j] );
		else printf( ",%d", array[j] );
	}
	fflush(0);
}

uint32_t array_crc( BigArrayPtr array, int len ){
	uint32_t crc = 0;
	int initial;
	if ( len >=1 ){
		crc = (crc+array[0]) % 1000000;
	}
	else return 1; //empty array always sorted
	for ( int i=1; i < len; i++ )
	{
		crc = (crc+array[i]) % 1000000;
	}
	return crc;
}

int test_sort_result( const BigArrayPtr unsorted, const BigArrayPtr sorted, int len ){
	uint32_t unsorted_crc = 0;
	uint32_t sorted_crc = 0;
	int initial;
	if ( len >=1 ){
		initial = sorted[0];
		unsorted_crc = (unsorted_crc+unsorted[0]) % 1000000;
		sorted_crc = (sorted_crc+sorted[0]) % 1000000;
	}
	else return 1;
	for ( int i=1; i < len; i++ ){
		unsorted_crc = (unsorted_crc+unsorted[i]) % 1000000;
		sorted_crc = (sorted_crc+sorted[i]) % 1000000;

		if ( initial > sorted[i] ) return 0;
		else initial = sorted[i];
	}

	//crc test
	if ( unsorted_crc != sorted_crc )
		return 0;

	return 1;
}

int run_sort( BigArrayPtr *unsorted, BigArrayPtr *sorted, int sortlen )
{
	*unsorted = alloc_array_fill_random( sortlen );
	*sorted = alloc_merge_sort( *unsorted, sortlen );

	if ( test_sort_result( *unsorted, *sorted, sortlen ) )
		return 1;
	else
		return 0;
	return 0;
}





