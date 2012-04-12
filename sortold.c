/*
 * sort.c
 *
 *  Created on: 16.03.2012
 *      Author: yaroslav
 */


#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#define ARRAY_ITEMS_COUNT 5000000

typedef uint32_t* BigArrayPtr;
typedef uint32_t  BigArrayItem;

BigArrayPtr __global_array;
BigArrayPtr __global_left;
BigArrayPtr __global_right;


BigArrayPtr merge_sort( int global_array_index, BigArrayPtr array, int array_len );
BigArrayPtr merge( int global_array_index,
		BigArrayPtr left_array, int left_array_len,
		BigArrayPtr right_array, int right_array_len );
void copy_array( BigArrayPtr dst_array, const BigArrayPtr src_array, int array_len );
BigArrayPtr alloc_copy_array( const BigArrayPtr array, int array_len );
void print_array(const char* text, BigArrayPtr array, int len);
int test_sort_result( BigArrayPtr unsorted, BigArrayPtr sorted, int len );


/**@param global_array_index index of item oaffest in array where sholud be result saved*/
BigArrayPtr merge_sort( int global_array_index, BigArrayPtr array, int array_len )
{
	if ( array_len <= 1 )
	{
		copy_array( __global_array+global_array_index, array, array_len );
		return __global_array+global_array_index;
	}

	int middle = array_len/2;
	BigArrayPtr left = merge_sort( global_array_index, array, middle );
	//print_array( "\nleft", array, middle );
	BigArrayPtr right = merge_sort( global_array_index+middle, array+middle, array_len-middle );
	//print_array( "\nright", array+middle, array_len-middle );

	copy_array( __global_left, left, middle );
	copy_array( __global_right, right, array_len-middle );

	BigArrayPtr result = merge( global_array_index, __global_left, middle,
			__global_right, array_len-middle );
	return result;
}

/**@param global_array_index is used to save result to correct place*/
BigArrayPtr merge( int global_array_index,
		BigArrayPtr left_array, int left_array_len,
		BigArrayPtr right_array, int right_array_len )
{
	//printf("\nglobal_array_index %d", global_array_index);
	int current_result_index = 0;
	while ( left_array_len > 0 && right_array_len > 0 )
	{
		if ( left_array[0] <= right_array[0]  )
		{
			//printf("\nleft[0]<= %d", left_array[0] );
			__global_array[global_array_index+current_result_index++] = left_array[0];
			++left_array;
			--left_array_len;
		}
		else
		{
			//printf("\nright[0]<= %d", right_array[0] );
			__global_array[global_array_index+current_result_index++] = right_array[0];
			++right_array;
			--right_array_len;
		}
	}

	//printf("\nleft_array_len %d, right_array_len %d", left_array_len, right_array_len);
	//printf("\nindex %d", global_array_index+current_result_index );

	//if merge arrays not empty then it can hold last item
	if ( left_array_len > 0 )
	{
		copy_array( __global_array+global_array_index+current_result_index++,
				left_array, left_array_len );
	}
	if ( right_array_len > 0 )
	{
		copy_array( __global_array+global_array_index+current_result_index++,
				right_array, right_array_len );
	}

	//print_array( "\nMERGE array=", __global_array, ARRAY_ITEMS_COUNT);
	return __global_array + global_array_index;
}


void copy_array( BigArrayPtr dst_array, const BigArrayPtr src_array, int array_len )
{
	for ( int i=0; i < array_len; i++ )
		dst_array[i] = src_array[i];
}


BigArrayPtr alloc_copy_array( const BigArrayPtr array, int array_len )
{
	BigArrayPtr newarray = malloc( sizeof(BigArrayItem)*array_len );
	for ( int i=0; i < array_len; i++ )
		newarray[i] = array[i];
	return newarray;
}

void print_array(const char* text, BigArrayPtr array, int len)
{
	puts(text);
	for (int j=0; j<len; j++)
	{
		if ( !j ) printf( "%d", array[j] );
		else printf( ",%d", array[j] );
	}
	fflush(0);
}

int test_sort_result( const BigArrayPtr unsorted, const BigArrayPtr sorted, int len )
{
	uint32_t unsorted_crc = 0;
	uint32_t sorted_crc = 0;
	int initial;
	if ( len >=1 )
		{
			initial = sorted[0];
			unsorted_crc = (unsorted_crc+unsorted[0]) % 1000000;
			sorted_crc = (sorted_crc+sorted[0]) % 1000000;
		}
	else return 1;
	for ( int i=1; i < len; i++ )
	{
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


int main()
{
	__global_left = malloc( sizeof(BigArrayItem)*(ARRAY_ITEMS_COUNT/2 +1) );
	__global_right = malloc( sizeof(BigArrayItem)*(ARRAY_ITEMS_COUNT/2 +1) );
	__global_array = malloc( sizeof(BigArrayItem)*ARRAY_ITEMS_COUNT );
	BigArrayPtr unsorted_array = malloc( sizeof(BigArrayItem)*ARRAY_ITEMS_COUNT );

	//fill array by random numbers
	srand(time(NULL));
	for (int i=0; i<ARRAY_ITEMS_COUNT; i++)
		unsorted_array[i]=rand();

	printf( "sorting %d items", ARRAY_ITEMS_COUNT );
	fflush(0);

	//print_array( "\nunsorted array=", unsorted_array, ARRAY_ITEMS_COUNT);

	__global_array = merge_sort( 0, unsorted_array, ARRAY_ITEMS_COUNT );

	//print_array( "\nsorted array=", __global_array, ARRAY_ITEMS_COUNT);

	if ( test_sort_result( unsorted_array, __global_array, ARRAY_ITEMS_COUNT ) )
		puts("\nsort OK\n");
	else
	{
		puts("\nsort FAILED\n");
		if ( ARRAY_ITEMS_COUNT < 100 )
			{
				print_array( "\nunsorted array=", unsorted_array, ARRAY_ITEMS_COUNT);
				print_array( "\nsorted array=", __global_array, ARRAY_ITEMS_COUNT);
			}
	}

	free( unsorted_array );
	free(__global_array);
	free(__global_right);
	free(__global_left);
	return 0;
}

