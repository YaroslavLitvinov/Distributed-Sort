/*
 * main.c
 *
 *  Created on: 17.03.2012
 *      Author: YaroslavLitvinov
 *      Distributed Sort uses the local sorting of each source data, then using ZeroMQ library for
 *      inter process communication to do exchange of data between nodes.
 *      Sorting system consists of a manager who initiates the sorting process, coordinates the work of
 *      source nodes that is suppliers of sorting data & destination nodes who receives results.
 */

#include "sort.h"

#include <zmq.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

//#define DEBUG

/* How many source nodes do we want? */
#define SRC_NODES_COUNT 5
/* Destination nodes count it's equal to sources */
#define DST_NODES_COUNT SRC_NODES_COUNT
/*Source data length stored in single source node (process)*/
#define ARRAY_ITEMS_COUNT 1000000
/*Identifiers of packets sending beetwen nodes*/
enum packet_t { EPACKET_UNKNOWN=-1, EPACKET_HISTOGRAM, EPACKET_SEQUENCE_REQUEST, EPACKET_RANGE, EPACKET_PID };

#define max(a,b) \
  ({ __typeof__ (a) _a = (a); \
      __typeof__ (b) _b = (b); \
    _a > _b ? _a : _b; })

#define min(a,b) \
  ({ __typeof__ (a) _a = (a); \
      __typeof__ (b) _b = (b); \
    _a < _b ? _a : _b; })


struct node_pid_t{
	pid_t src_node_pid;
	pid_t dst_node_pid;
};

struct sort_result{
	pid_t pid;
	BigArrayItem min;
	BigArrayItem max;
	uint32_t crc;
};

/**It used by sorting protocol*/
struct packet_data_t{
	int type; //packet_t enum
	size_t size; //size of next packet
	pid_t src_pid;
};


struct Histogram{
	pid_t src_pid;
	size_t array_len;
	HistogramArrayPtr array;
};


struct request_data_t{
	int first_item_index;
	int last_item_index;
	pid_t src_pid;
	pid_t dst_pid;
};

struct histogram_helper_t{
	int begin_offset;
	int begin_histogram_index; //First histogram in range
	int end_histogram_index; //Last histogram in range
	int begin_detailed_histogram_index; //First histogram in range
	int end_detailed_histogram_index; //Last histogram in range
};


struct histogram_worker{
	struct Histogram histogram;
	struct Histogram detailed_histogram;
	struct histogram_helper_t helper;
	int current_histogram_complete;
};


/*@return 1-should receive again, 0-complete request - it should not be listen again*/
int
channel_recv_detailed_histograms_request(void *context, const BigArrayPtr source_array, int array_len);

/*@param complete Flag 0 say to client in request that would be requested again, 1-last request send
 *return Histogram Caller is responsive to free memory after using result*/
struct Histogram*
channel_request_response_detailed_histograms_alloc_get_len(void *context, const struct request_data_t* request_data,
		int request_array_len, int complete );


void
init_worker( struct histogram_worker* worker ){
	worker->detailed_histogram.array = NULL;
	worker->helper.begin_histogram_index = 0;
	worker->helper.end_histogram_index = 0;
	worker->helper.begin_detailed_histogram_index = 0;
	worker->helper.end_detailed_histogram_index = 0;
	worker->helper.begin_offset = 0;
	worker->current_histogram_complete = 0;
}


void
check_remove_detailed_histogram( struct histogram_worker* worker ){
	if ( worker->detailed_histogram.array ){
		HistogramArrayItem current_item = worker->detailed_histogram.array[worker->helper.end_detailed_histogram_index];
		int index = 0;
		for ( int i=worker->helper.end_histogram_index; i < worker->histogram.array_len; i++ ){
			if ( worker->histogram.array[i].item_index >= current_item.item_index ){
				index = i;
				break;
			}
		}
		/*if current item of detailed_histogram is last item from detailed_histogram can be synchronized
				e.g. equal to one of big histogram items then synchronize it;*/
		if ( worker->histogram.array[index].item_index == current_item.item_index ){
			worker->helper.begin_offset = worker->detailed_histogram.array[ worker->helper.begin_detailed_histogram_index ].item_index;
			worker->helper.begin_histogram_index = worker->helper.end_histogram_index = index;
			/*detailed histogram currently no needed, discard detailed histogram*/
			free( worker->detailed_histogram.array );
			worker->detailed_histogram.array = NULL;
		}
	}
}


void
init_histogram( struct histogram_worker* worker ){
	worker->helper.begin_offset = 0;
	if ( worker->detailed_histogram.array ){
		worker->helper.begin_histogram_index = -1; //uninitialized can't be used
		worker->helper.begin_detailed_histogram_index = worker->helper.end_detailed_histogram_index;
	}else{
		worker->helper.begin_histogram_index = worker->helper.end_histogram_index;
	}
}

void
set_detailed_histogram( struct histogram_worker* worker, struct Histogram* detailed_histogram  ){
	assert( !worker->detailed_histogram.array ); /*test: should not be previous detailed histograms*/
	if ( detailed_histogram && detailed_histogram->array_len ){
		int big_histogram_first_item_index = worker->histogram.array[worker->helper.end_histogram_index].item_index;
		/*test: 0 item index of 'detailed histogram' should be equal to last item index of histogram*/
		assert( detailed_histogram->array[0].item_index == big_histogram_first_item_index );
		free(worker->detailed_histogram.array);
		worker->detailed_histogram.array = NULL;
		worker->detailed_histogram = *detailed_histogram;
		/*pointing to begin of detailed histogram*/
		worker->helper.begin_detailed_histogram_index = worker->helper.end_detailed_histogram_index=0;
	}
}

void
set_next_histogram( struct histogram_worker* worker ){
	if ( worker->detailed_histogram.array ){
		if ( worker->helper.end_detailed_histogram_index+1 == worker->detailed_histogram.array_len )
			worker->current_histogram_complete = 1; //flag
		if ( ! worker->current_histogram_complete )
			worker->helper.end_detailed_histogram_index++;
	}
	else
		worker->helper.end_histogram_index++;
}


HistogramArrayPtr
value_at_cursor_histogram( struct histogram_worker* worker ){
	struct Histogram *histogram = &worker->histogram;
	int *end_histogram_index = &worker->helper.end_histogram_index;
	if ( worker->detailed_histogram.array ){
		histogram = &worker->detailed_histogram;
		end_histogram_index = &worker->helper.end_detailed_histogram_index;
	}

	if ( !worker->current_histogram_complete && *end_histogram_index < histogram->array_len){
		return &histogram->array[ *end_histogram_index ];
	}
	return 0;
}


int
length_current_histogram( const struct histogram_worker* worker ){
	const struct Histogram* histogram = &worker->histogram;
	const int *end_histogram_index = &worker->helper.end_histogram_index;
	if ( worker->detailed_histogram.array ){
		histogram = &worker->detailed_histogram;
		end_histogram_index = &worker->helper.end_detailed_histogram_index;
	}
	return histogram->array[ *end_histogram_index ].last_item_index - histogram->array[ *end_histogram_index ].item_index + 1;
}

void
get_begin_end_histograms_item_indexes( const struct histogram_worker* worker, int *first_item_index, int *end_item_index ){
	int begin_detail_index = worker->helper.begin_detailed_histogram_index;
	int begin_index = worker->helper.begin_histogram_index;
	if ( worker->detailed_histogram.array )	{
		int min1 = worker->detailed_histogram.array[ begin_detail_index ].item_index;
		if ( worker->helper.begin_offset > 0 ){
			min1 = min( worker->helper.begin_offset, worker->detailed_histogram.array[ begin_detail_index ].item_index );
		}
		if ( begin_index != -1 ){
			min1 = min( min1, worker->histogram.array[ begin_index ].item_index );
		}
		*first_item_index = min1;
		*end_item_index = worker->detailed_histogram.array[worker->helper.end_detailed_histogram_index].item_index;
		if ( worker->current_histogram_complete )
			*end_item_index = *end_item_index+1;
	}
	else
	{
		if ( worker->helper.begin_offset > 0 ){
			*first_item_index = min( worker->helper.begin_offset, worker->histogram.array[ begin_index ].item_index );
		}
		else
			*first_item_index = worker->histogram.array[ begin_index ].item_index;
		*end_item_index = worker->histogram.array[worker->helper.end_histogram_index].item_index;
	}
}

int
size_all_processed_histograms( const struct histogram_worker* array_workers, int array_len ){
	int count = 0;
	int begin_index = 0;
	int end_index = 0;
	for ( int i=0; i < array_len; i++ )	{
		const struct histogram_worker* worker = &array_workers[i];
		begin_index = 0;
		end_index = 0;
		get_begin_end_histograms_item_indexes( worker, &begin_index, &end_index );
		count += end_index - begin_index;
	}
	return count;
}


void
print_request_data_array( struct request_data_t* const range, int len ){
	for ( int j=0; j < len; j++ )
	{
		printf("SEQUENCE N:%d, dst_pid=%d, src_pid=%d, findex %d, lindex %d \n",
				j, (int)range[j].dst_pid, (int)range[j].src_pid, range[j].first_item_index, range[j].last_item_index );
	}
}


void
init_helper_array( struct histogram_helper_t *range, int len ){
	for ( int j=0; j < len; j++ ){
		range[j].begin_histogram_index=0;
		range[j].end_histogram_index=0;
		range[j].begin_offset = 0;
	}
}

void
init_request_data_array( struct request_data_t *req_data, int len ){
	for ( int j=0; j < len; j++ ){
		req_data[j].src_pid = 0;
		req_data[j].dst_pid = 0;
		req_data[j].first_item_index = 0;
		req_data[j].last_item_index = 0;
	}
}

void
request_assign_detailed_histogram( void *context, int current_histogram_len,
		struct histogram_worker* workers, int array_len, int last_request ){
	pid_t pid = getpid();
	struct request_data_t request_detailed_histogram[array_len];
	for (int i=0; i < array_len; i++){
		int start_index =
				workers[i].histogram.array[workers[i].helper.end_histogram_index].item_index;

		request_detailed_histogram[i].dst_pid = workers[i].histogram.src_pid;
		request_detailed_histogram[i].src_pid = pid;
		request_detailed_histogram[i].first_item_index = start_index;
		request_detailed_histogram[i].last_item_index =
				min(start_index + current_histogram_len * array_len, ARRAY_ITEMS_COUNT );
#ifdef DEBUG
		printf("\nWant %d range(%d, %d)\n",
				request_detailed_histogram[i].dst_pid,
				request_detailed_histogram[i].first_item_index,
				request_detailed_histogram[i].last_item_index ); fflush(0);
#endif
	}

	struct Histogram* detailed_histogram = channel_request_response_detailed_histograms_alloc_get_len(
			context, request_detailed_histogram, array_len, last_request );
	//save received detailed histograms into workers array
	for ( int i=0; i < array_len; i++ ){
		set_detailed_histogram( &workers[i], &detailed_histogram[i] );
	}
	/*free array memory, pointer data on cells is untouched
	 *detailed_histogram items should be deleted after use*/
	free(detailed_histogram); //free array cells, data on cells is un\touched

}


struct request_data_t**
alloc_range_request_analize_histograms( void *context,
		const struct Histogram *histograms_array, size_t len, struct node_pid_t *child, int child_len ){
	pid_t pid = getpid();
	struct request_data_t **result = NULL;
	struct histogram_worker workers[len];
	for ( int i=0; i < len; i++ ){
		workers[i].histogram = histograms_array[i];
		init_worker(&workers[i]);
	}
	int destination_index = 0;
	int source_index_of_histogram = -1;
	int allow_check_remove_detailed_hitogram = 0;
	do{
		int last_histograms_requested = 0;
		int range_count = 0; /*items count processed for all destinations, max=ARRAY_ITEMS_COUNT*len*/
		if ( destination_index > 0 ){
			for (int j=0; j < len; j++){
				init_histogram( &workers[j] );
			}
		}
		while( range_count < ARRAY_ITEMS_COUNT ){
			source_index_of_histogram = -1;
			int is_valid_current_item = 0;

			/*in case when histogram item of big histogram is equal to item of detailed histogram
			then now switch from detailed to big histogram */
			for (int i=0; i < len; i++){
				/*Last requested detailed histograms should not be deleted to use it's data to completion of Analize*/
				if ( !last_histograms_requested && allow_check_remove_detailed_hitogram )
					check_remove_detailed_histogram( &workers[i] );
			}

			/*search histogram in array of histogram which terms to condition*/
			/*cycle for histograms*/
			HistogramArrayItem min_value; min_value.item = 0;
			HistogramArrayPtr current_value;
			for ( int i=0; i < len; i++ ){
				current_value = value_at_cursor_histogram( &workers[i] );
				if ( !current_value ) continue;
				/*set first item as default minimum value*/
				if ( -1 == source_index_of_histogram ){
					min_value = *current_value;
				}
				//check both current begin & end are in minimum range
				if ( current_value->item <= min_value.item )
				{
					min_value = *current_value;
					source_index_of_histogram = i; /*save source index to use this histogram*/
				}
			}
#ifdef DEBUG
			assert( -1 != source_index_of_histogram );
#endif
			set_next_histogram( &workers[source_index_of_histogram] ); /*move cursor to next histogram*/
			range_count = size_all_processed_histograms( workers, len );

			//if up to end of ARRAY_ITEMS_COUNT range less than len histograms
			//so request histograms with step=1
			int histogram_len = length_current_histogram( &workers[source_index_of_histogram] );
			if ( !workers[source_index_of_histogram].detailed_histogram.array &&
				 range_count + len*histogram_len >= ARRAY_ITEMS_COUNT)
			{
				last_histograms_requested = destination_index+1 >= len;
				printf("\r#%d Detailed Histograms recv start\n", destination_index );fflush(0);
				request_assign_detailed_histogram( context, histogram_len, workers, len, last_histograms_requested );
				printf("\r#%d Detailed Histograms received\n", destination_index );fflush(0);
				allow_check_remove_detailed_hitogram = 0;
			}
		} //while
		/*save range data based on histograms*/
		if ( !result )
			result = malloc( sizeof(struct request_data_t*)*len ); /*alloc memory for pointers*/
		result[destination_index] = malloc( sizeof(struct request_data_t)*len ); /*alloc memory for one-dimension array*/
		/*save results*/
		for (int j=0; j < len; j++){
			int first_item_index = 0;
			int last_item_index = 0;
			get_begin_end_histograms_item_indexes( &workers[j], &first_item_index, &last_item_index );
			result[destination_index][j].first_item_index = first_item_index;
			result[destination_index][j].last_item_index = last_item_index-1;
			result[destination_index][j].src_pid = workers[j].histogram.src_pid;
			for (int k=0; k < child_len; k++)
				if ( histograms_array[j].src_pid == child[k].src_node_pid  )
				{
					result[destination_index][j].dst_pid = child[k].dst_node_pid;
				}
		}

		allow_check_remove_detailed_hitogram = 1;
		destination_index++;
	}while( destination_index < len );

	for ( int i=0; i < len; i++ ){
		free(workers[i].detailed_histogram.array);
	}

	return result;
}


/**
 * socket existing zmq read socket
 * @return message size
 * */
size_t
receive_message_check( void *socket, void *message, size_t waiting_size ){
	pid_t pid = getpid();
	zmq_msg_t msg;
	zmq_msg_init (&msg);
	zmq_recv (socket, &msg, 0);
	size_t msg_size = zmq_msg_size (&msg);
	void *msg_data = zmq_msg_data (&msg);
	if ( waiting_size == msg_size ){
		memcpy (message, msg_data, msg_size);
		/*printf("receive_message_check[%d] %d\n", (int)pid, (int)msg_size);*/
	}
	else{
		printf("receive_message_check[%d]:wrong size msg_size=%d, waiting_size=%d\n",
				(int)pid, (int)msg_size, (int)waiting_size );
		exit(0);
	}
	return msg_size;
}

void*
alloc_receive_message_get_size( void *socket, size_t *size ){
	zmq_msg_t msg;
	zmq_msg_init (&msg);
	zmq_recv (socket, &msg, 0);
	void *message = NULL;
	*size = zmq_msg_size (&msg);
	message = malloc( *size );
	memcpy (message, zmq_msg_data (&msg), *size);
	return message;
}


/**
 * socket existing zmq write socket
 * */
void
transmit_message( void *socket, const void *message, size_t size, int option ){
	zmq_msg_t msg;
	zmq_msg_init_size (&msg, size);
	memcpy (zmq_msg_data (&msg), message, size);
	zmq_send (socket, &msg, option);
	zmq_msg_close (&msg);
}

void
channel_receive_sorted_ranges(  void *context, BigArrayPtr dst_array, int dst_array_len, int ranges_count ){
	pid_t pid = getpid();
	int recv_bytes_count = 0;

	void *reader = zmq_socket(context, ZMQ_REP);
	char transport[30];
	sprintf( transport, "ipc://range%d",(int)pid );
	zmq_bind(reader, transport);
#ifdef DEBUG
	printf("[%d] Recv ranges by %s\n", (int)pid, transport);
#endif
	for (int i=0; i < ranges_count; i++)
	{
#ifdef DEBUG
		printf("%s recv array -->", transport);
#endif
		zmq_msg_t array_msg;
		zmq_msg_init(&array_msg);
		zmq_recv (reader, &array_msg, 0);
		const size_t msg_size = zmq_msg_size(&array_msg);
		memcpy( &dst_array[recv_bytes_count/sizeof(BigArrayItem)], zmq_msg_data (&array_msg), msg_size);
		recv_bytes_count += msg_size;
		zmq_msg_close (&array_msg);
#ifdef DEBUG
		printf("--size=%d %s OK\n", (int)msg_size, transport );
#endif
		char reply='-';
		transmit_message( reader, &reply, 1, 0 );
#ifdef DEBUG
		printf("\n[%d]Send reply to %s\n", (int)pid, transport);
#endif
	}
	zmq_close(reader);
	assert( dst_array_len*sizeof(BigArrayItem) == recv_bytes_count );
#ifdef DEBUG
	printf("[%d] channel_receive_sorted_ranges OK\n", (int)pid );
#endif
}


void
channel_send_sorted_ranges( void *context, const struct request_data_t* sequence, int sequence_len,
		const BigArrayPtr src_array, int src_array_len ){
	pid_t pid = getpid();

	for ( int i=0; i < sequence_len; i++ )
	{
		void *writer = zmq_socket(context, ZMQ_REQ);
		char transport[30];
		//sprintf( transport, "ipc://range%d-%d", (int)pid, (int)sequence[i].dst_pid  );
		sprintf( transport, "ipc://range%d", (int)sequence[i].dst_pid  );
		int rc = zmq_connect(writer, transport);

		const int array_len = sequence[i].last_item_index - sequence[i].first_item_index + 1;
		const size_t array_size = array_len*sizeof(BigArrayItem);
		const BigArrayPtr array = src_array+sequence[i].first_item_index;
#ifdef DEBUG
		printf("\n[%d]Sending array_size=%d; min=%d, max=%d via %s\n",
				(int)pid, (int)array_size, array[0], array[array_len-1], transport);
#endif
		transmit_message( writer, array, array_size, 0 );
#ifdef DEBUG
	   	printf("\n[%d]Waiting receiver reply; via %s\n", (int)pid, transport);
#endif
	   	char reply;
	   	receive_message_check( writer, &reply, 1 );
#ifdef DEBUG
	   	printf("\n[%d]Reply from receiver OK; via %s\n", (int)pid, transport);
#endif
		zmq_close(writer);
	}
#ifdef DEBUG
	printf("\n[%d]Sending Complete-OK\n", (int)pid);
#endif
}


/**@param dst_pid destination process should receive ranges*/
int
channel_recv_sequences_request( void *context, struct request_data_t* sequence, pid_t *dst_pid ){
	int len = 0;
	pid_t pid = getpid();
	void *reader = zmq_socket(context, ZMQ_PULL);
	char transport[30];
	sprintf( transport, "ipc://range-request-%d", (int)pid );
	zmq_bind(reader, transport);

#ifdef DEBUG
	printf("receiving seqreq via transport %s\n", transport); fflush(0);
#endif

	struct packet_data_t t;
	t.type = EPACKET_UNKNOWN;
	receive_message_check( reader, &t, sizeof(t) );
	*dst_pid = t.src_pid;

	if ( t.type == EPACKET_SEQUENCE_REQUEST )
	{
		for ( int j=0; j < t.size; j++ ){
			pid_t src_pid = 0, dst_pid = 0;
			int findex = 0;
			int lindex = 0;
			receive_message_check( reader, &src_pid, sizeof(src_pid) ); /*SRC PID process */
			receive_message_check( reader, &dst_pid, sizeof(dst_pid) ); /*DST PID process */
			receive_message_check( reader, &findex,  sizeof(findex) ); /*first item index in sequence */
			receive_message_check( reader, &lindex,  sizeof(lindex) ); /*last item index in sequence */
			sequence[j].src_pid = src_pid;
			sequence[j].dst_pid = dst_pid;
			sequence[j].first_item_index = findex;
			sequence[j].last_item_index = lindex;
#ifdef DEBUG
			printf("recvseq %d %d %d\n", (int)src_pid, findex, lindex );
#endif
		}
	}
	else{
		perror("channel_recv_sequences_request::packet Unknown");
	}
	zmq_close(reader);
	return len;
}



void
channel_send_sequences_request( void *context, struct request_data_t** range, struct node_pid_t *child, int len ){
	pid_t pid = getpid();
	for (int i=0; i < len; i++ ){
		void *writer = zmq_socket(context, ZMQ_PUSH);
		char transport[30];
		sprintf( transport, "ipc://range-request-%d", (int)range[0][i].src_pid);
		zmq_connect(writer, transport);
#ifdef DEBUG
		printf("sending seqreq via transport %s\nchannel_send_sequences_request", transport); fflush(0);
#endif
		struct packet_data_t t;
		t.type = EPACKET_SEQUENCE_REQUEST;
		t.src_pid = (int)range[i][0].dst_pid; //use pid of related dst process
		t.size = len;

		transmit_message( writer, &t, sizeof(t), 0 );
		for ( int j=0; j < len; j++ ){
			pid_t src = range[j][i].src_pid;
			pid_t dst = child[j].dst_node_pid;
			int findex = range[j][i].first_item_index;
			int lindex = range[j][i].last_item_index;
			transmit_message( writer, &src, sizeof(src), ZMQ_SNDMORE ); /*SRC PID process */
			transmit_message( writer, &dst, sizeof(src), ZMQ_SNDMORE ); /*DST PID process */
			transmit_message( writer, &findex, sizeof(findex), ZMQ_SNDMORE ); /*first item index in sequence */
			transmit_message( writer, &lindex, sizeof(lindex), 0 ); /*last item index in sequence */
#ifdef DEBUG
			printf("sendseq %d %d %d\n", (int)src, findex, lindex );
#endif
		}

		zmq_close(writer);
	}


}

/*@return 1-should receive again, 0-complete request - it should not be listen again*/
int
channel_recv_detailed_histograms_request(void *context, const BigArrayPtr source_array, int array_len){
	int is_complete = 0;
	pid_t pid = getpid();
	void *socket = zmq_socket(context, ZMQ_REP);
	char transport[30];
	sprintf( transport, "ipc://details-%d", pid );
	zmq_bind (socket, transport);

	do {
#ifdef DEBUG
		printf("\n[%d] Receiving detailed histograms request by %s\n", (int)pid, transport );fflush(0);
#endif
		/*receive data needed to create histogram using step=1,
		actually requested histogram should contains array items range*/
		struct request_data_t received_histogram_request;
		receive_message_check( socket, &received_histogram_request, sizeof(received_histogram_request) );
		receive_message_check( socket, &is_complete, sizeof(is_complete) );

		int histogram_len = 0;
		//set to our offset, check it
		int offset = min(received_histogram_request.first_item_index, array_len-1 );
		int requested_length = received_histogram_request.last_item_index - received_histogram_request.first_item_index;
		requested_length = min( requested_length, array_len - offset );

		HistogramArrayPtr histogram = alloc_histogram_array_get_len( source_array, offset, requested_length, 1, &histogram_len );

		size_t sending_array_len = histogram_len;
		/*Response to request, entire reply contains requested detailed histogram*/
		transmit_message( socket, &pid, sizeof(pid_t), ZMQ_SNDMORE );
		transmit_message( socket, &sending_array_len, sizeof(size_t), ZMQ_SNDMORE );
		transmit_message( socket, histogram, histogram_len*sizeof(HistogramArrayItem), 0 );
		free( histogram );
#ifdef DEBUG
		printf("\n[%d] histograms sent by %s\n", (int)pid, transport );fflush(0);
#endif
	}while(!is_complete);
	zmq_close(socket);
	return is_complete;
}


/*@param complete Flag 0 say to client in request that would be requested again, 1-last request send
 *return Histogram Caller is responsive to free memory after using result*/
struct Histogram*
channel_request_response_detailed_histograms_alloc_get_len(void *context, const struct request_data_t* request_data,
		int request_array_len, int complete ){
	pid_t pid = getpid();
	//alloc histograms array with items count should be requested/received
	struct Histogram* detailed_histograms = malloc( sizeof(struct Histogram)*request_array_len );

	for( int i=0; i < request_array_len; i++ ){
		void *socket = zmq_socket(context, ZMQ_REQ);
		char transport[30];
		sprintf( transport, "ipc://details-%d", request_data[i].dst_pid );
		zmq_connect (socket, transport);
#ifdef DEBUG
		printf("\n[%d] complete=%d, Sending detailed histogram requests by %s\n", (int)pid, complete, transport );fflush(0);
#endif
		//send detailed histogram request
		transmit_message( socket, &request_data[i], sizeof(struct request_data_t), ZMQ_SNDMORE );
		transmit_message( socket, &complete, sizeof(complete), 0 );
#ifdef DEBUG
		printf("\n[%d] detailed histograms receiving\n", (int)pid );fflush(0);
#endif
		//recv reply
		struct Histogram item;
		receive_message_check( socket, &item.src_pid, sizeof(item.src_pid) );
		receive_message_check( socket, &item.array_len, sizeof(item.array_len) );
		size_t received_array_size;
		item.array = alloc_receive_message_get_size( socket, &received_array_size );

#ifdef DEBUG
		printf("\n[%d] detailed histograms received from%d: expected len:%d, received len:%d\n",
				pid, item.src_pid, (int)(sizeof(HistogramArrayItem)*item.array_len), (int)received_array_size );fflush(0);
#endif
		detailed_histograms[i] = item;
		zmq_close(socket);
	}
	return detailed_histograms;
}


void
channel_recv_histograms( void *context, struct Histogram *histograms, int wait_number ){
	void *reader = zmq_socket(context, ZMQ_PULL);
	zmq_bind (reader, "ipc://histogram");

	for( int i=0; i < wait_number; i++ ){
		struct packet_data_t t; t.type = EPACKET_UNKNOWN;
		size_t size = receive_message_check( reader, &t, sizeof(t) );
		size_t array_size;

		if ( EPACKET_HISTOGRAM == t.type ){
			histograms[i].array = alloc_receive_message_get_size( reader, &array_size );
			histograms[i].array_len = array_size / sizeof(HistogramArrayItem);
			histograms[i].src_pid = t.src_pid;
		}
		else if ( size ){
			printf("channel_recv_histogram::wrong packet type %d size %d", t.type, (int)t.size);
			exit(-1);
		}
	}
	zmq_close(reader);
}


void
channel_send_histogram( void *context, const struct Histogram *histogram ){
	void *writer = zmq_socket(context, ZMQ_PUSH);
	zmq_connect (writer, "ipc://histogram");

	size_t array_size = sizeof(HistogramArrayItem)*(histogram->array_len);
	struct packet_data_t t;
	t.type = EPACKET_HISTOGRAM;
	t.src_pid = histogram->src_pid;
	t.size = array_size;

	transmit_message(writer, &t, sizeof(t), ZMQ_SNDMORE);
	transmit_message(writer, histogram->array, array_size, 0);

	zmq_close(writer);
}


pid_t*
channel_recv_source_pids_get_len( void *context, int *pids_len ){
	pid_t pid = getpid();
	void *reader = zmq_socket(context, ZMQ_PULL);
	char transport[30];
	sprintf(transport, "ipc://pids-%d", (int)pid);
	zmq_bind(reader, transport);

	struct packet_data_t t;
	zmq_msg_t packet_msg;
	zmq_msg_init (&packet_msg);
	zmq_recv (reader, &packet_msg, 0);
	int size = zmq_msg_size(&packet_msg);
	memcpy(&t, zmq_msg_data (&packet_msg), sizeof(t));
	zmq_msg_close(&packet_msg);
	if ( t.type != EPACKET_PID ){
		printf("[%d]wrong_packet %d, size=%d\n", (int)pid, t.type, size);fflush(0);
		perror("channel_recv_source_pids::Wrong packet");
	}
	pid_t *pids = malloc( t.size );
	int pid_i = 0;
	int recv_bytes_count = 0;
	while( recv_bytes_count < t.size ){
		zmq_msg_t pid_msg;
		zmq_msg_init (&pid_msg);
		zmq_recv (reader, &pid_msg, 0);
		memcpy(&pids[pid_i++], zmq_msg_data (&pid_msg), sizeof(pid_t));
		recv_bytes_count+=zmq_msg_size(&pid_msg);
		zmq_msg_close(&pid_msg);
	}
	zmq_close(reader);
	*pids_len = t.size / sizeof(pid_t);
	return pids;
}


void
channel_send_source_pids( void *context, const struct node_pid_t* pids, int pids_len ){
	for( int j=0; j < pids_len; j++ ){
		void *writer = zmq_socket(context, ZMQ_PUSH);
		char transport[30];
		sprintf(transport, "ipc://pids-%d", (int)pids[j].dst_node_pid);
		zmq_connect(writer, transport);

		struct packet_data_t t;
		t.type = EPACKET_PID;
		t.size = pids_len*sizeof(pid_t);
		transmit_message(writer, &t, sizeof(t), 0);
		for( int i=0; i < pids_len; i++ ){
			transmit_message(writer, &pids[i].src_node_pid, sizeof(pid_t), 0);
		}
		zmq_close(writer);
	}
}


void
send_sort_result( void *context, BigArrayPtr sorted_array, int len ){
	if ( !len ) return;
	pid_t pid = getpid();
	void *writer = zmq_socket(context, ZMQ_PUSH);
	zmq_connect(writer, "ipc://sort-result");

	uint32_t sorted_crc = array_crc( sorted_array, ARRAY_ITEMS_COUNT );

	transmit_message( writer, &pid, sizeof(pid), ZMQ_SNDMORE );
	transmit_message( writer, &sorted_array[0], sizeof(BigArrayItem), ZMQ_SNDMORE );
	transmit_message( writer, &sorted_array[len-1], sizeof(BigArrayItem), ZMQ_SNDMORE );
	transmit_message( writer, &sorted_crc, sizeof(sorted_crc), 0 );
#ifdef DEBUG
	printf( "[%d] send_sort_result: min=%d, max=%d, crc=%u\n", pid, sorted_array[0], sorted_array[len-1], sorted_crc );
#endif
	zmq_close( writer );
}


struct sort_result*
recv_sort_result( void *context, int waiting_results ){
	if ( !waiting_results ) return NULL;
	void *reader = zmq_socket(context, ZMQ_PULL);
	zmq_bind(reader, "ipc://sort-result");

	struct sort_result *results = malloc( SRC_NODES_COUNT*sizeof(struct sort_result) );
	for ( int i=0; i < waiting_results; i++ ){
		receive_message_check( reader, &results[i].pid, sizeof(results[i].pid) );
		receive_message_check( reader, &results[i].min, sizeof(results[i].min) );
		receive_message_check( reader, &results[i].max, sizeof(results[i].max) );
		receive_message_check( reader, &results[i].crc, sizeof(results[i].crc) );
	}
	zmq_close( reader );
	return results;
}


void
result_entry_point( int dst_nodes_count ){
	pid_t pid = getpid();
	void *context = zmq_init(1);

	BigArrayPtr unsorted_array = NULL;
	BigArrayPtr sorted_array = NULL;

	/* Receiving process ids of source data supplier
	 * list of source ids is not using currently and can be removed*/
	int pids_len = 0;
	pid_t* pids = channel_recv_source_pids_get_len( context, &pids_len );
	/*---------------------------------------------*/

	unsorted_array = malloc( ARRAY_ITEMS_COUNT*sizeof(BigArrayItem) );
	channel_receive_sorted_ranges( context, unsorted_array, ARRAY_ITEMS_COUNT, SRC_NODES_COUNT );
	free(pids);

	sorted_array = alloc_merge_sort( unsorted_array, ARRAY_ITEMS_COUNT );

	//sort complete, test it
	send_sort_result( context, sorted_array, ARRAY_ITEMS_COUNT );

	free(unsorted_array);
	zmq_term(context);
}

void
source_entry_point( int src_nodes_count ){
	pid_t pid = getpid();
	//create context and bind socket
	void *context = zmq_init(SRC_NODES_COUNT);

	BigArrayPtr unsorted_array = NULL;
	BigArrayPtr partially_sorted_array = NULL;

	//if first part of sorting in single thread are completed
	if ( run_sort( &unsorted_array, &partially_sorted_array, ARRAY_ITEMS_COUNT ) ){
		uint32_t crc = array_crc( partially_sorted_array, ARRAY_ITEMS_COUNT );
		if ( ARRAY_ITEMS_COUNT ){
			printf("Single process sorting complete min=%d, max=%d: TEST OK.\n",
					partially_sorted_array[0], partially_sorted_array[ARRAY_ITEMS_COUNT-1] );
			fflush(0);
		}

		int histogram_len = 0;
		HistogramArrayPtr histogram_array = alloc_histogram_array_get_len(
				partially_sorted_array, 0, ARRAY_ITEMS_COUNT, 1000, &histogram_len );

		struct Histogram single_histogram;
		single_histogram.src_pid = pid;
		single_histogram.array_len = histogram_len;
		single_histogram.array = histogram_array;
		//send histogram to manager

		channel_send_histogram( context, &single_histogram );
#ifdef DEBUG
		printf( "Sent SRC[%d] Histogram:\n", single_histogram.src_pid );
		print_histogram( single_histogram.array, single_histogram.array_len );
		fflush(0);
#endif
		//recv histogram request until function return 0
		channel_recv_detailed_histograms_request(context, partially_sorted_array, ARRAY_ITEMS_COUNT);
#ifdef DEBUG
		printf("\n!!!!!!!Hisograms Sending complete!!!!!!.\n");
#endif
		pid_t dst_pid = 0;
		struct request_data_t req_data_array[SRC_NODES_COUNT];
		init_request_data_array( req_data_array, SRC_NODES_COUNT);
		channel_recv_sequences_request( context, req_data_array, &dst_pid );
		channel_send_sorted_ranges( context, req_data_array, SRC_NODES_COUNT, partially_sorted_array, ARRAY_ITEMS_COUNT );

		free(unsorted_array);
		free(partially_sorted_array);
	}
	else{
		printf("Single process sorting failed: TEST FAILED.\n");
		exit(0);
	}

	zmq_term(context);
}

static int
sortresult_comparator( const void *m1, const void *m2 )
{
	const struct sort_result *t1= (struct sort_result* const)(m1);
	const struct sort_result *t2= (struct sort_result* const)(m2);

	if ( t1->pid < t2->pid )
		return -1;
	else if ( t1->pid > t2->pid )
		return 1;
	else return 0;
	return 0;
}

/** Parralel sorting of arrays in several processes.
 * Application run N processes, every process has own part of unsorted array.
 * All array of each process has the same size. Summary array should be sorted in next way:
 * Every process has own sorted sequence of numbers where the last and the same time
 * an maximum number should below or equal to min number of array from next process.*/
int
main(int argc, char **argv){
	pid_t pid = getpid();
	struct node_pid_t child[SRC_NODES_COUNT];

	for (int i = 0; i < SRC_NODES_COUNT; i++) {

		child[i].src_node_pid = fork();

		if ( child[i].src_node_pid == 0 ) {
			/*it's child running, fork returned 0, it's CHILD act as Source node*/
			source_entry_point( SRC_NODES_COUNT );
			exit(-1);
		}
		else if ((int) child[i].src_node_pid < 0) {
			perror("fork"); /* something went wrong */
		}
		else{
			/*main process running*/
			printf("Forked off src node # %d with pid %d\n", i, child[i].src_node_pid);
		}
	}

	for (int i = 0; i < DST_NODES_COUNT; i++) {

		child[i].dst_node_pid = fork();

		if ( child[i].dst_node_pid == 0 ) {
			/*it's child running, fork returned 0, this CHILD act as Destination node*/
			result_entry_point( DST_NODES_COUNT );
			exit(-1);
		}
		else if ((int) child[i].dst_node_pid < 0) {
			perror("fork"); /* something went wrong */
		}
		else{
			/*main process running*/
			printf("Forked off dst node # %d with pid %d\n", i, child[i].dst_node_pid);
		}
	}

	/*Main process act as MANAGER*/

	void *context = zmq_init(1);

	/*send to destination nodes the list of src pid's
	 * It can be deleted because it's not used by destination nodes anymore*/
	channel_send_source_pids( context, child, SRC_NODES_COUNT );
	/*--------------------------------------------*/

	struct Histogram histograms[SRC_NODES_COUNT];
	int histogram_array_len = -1;

	channel_recv_histograms( context, histograms, SRC_NODES_COUNT );
	struct request_data_t** range = alloc_range_request_analize_histograms( context, histograms, SRC_NODES_COUNT,
			child, SRC_NODES_COUNT );

#ifdef DEBUG
	for (int i=0; i < SRC_NODES_COUNT; i++ )
	{
		printf( "SOURCE PART N %d:\n", i );
		print_request_data_array( range[i], SRC_NODES_COUNT );
	}
#endif

	channel_send_sequences_request( context, range, child, SRC_NODES_COUNT );

	for ( int i=0; i < SRC_NODES_COUNT; i++ ){
		free(histograms[i].array);
		free( range[i] );
	}
	free(range);

	struct sort_result *results = recv_sort_result( context, SRC_NODES_COUNT );
	qsort( results, SRC_NODES_COUNT, sizeof(struct sort_result), sortresult_comparator );
	int sort_ok = 1;
	for ( int i=0; i < SRC_NODES_COUNT; i++ ){
		if ( i>0 ){
			if ( !(results[i].max > results[i].min && results[i-1].max < results[i].min) )
				sort_ok = 0;
		}
		printf("results[%d], pid=%d, min=%d, max=%d\n",
				i, results[i].pid, results[i].min, results[i].max);
		fflush(0);
	}

	printf( "Distributed sort complete, Test %d\n", sort_ok );

	zmq_term (context);

	while (wait(NULL) > 0)	/* now parent waits for all children */
		;
	return 0;
}


