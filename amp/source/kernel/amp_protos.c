/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#include <amp_sys.h>
#include <amp_types.h>
#include <amp_protos.h>
#include <amp_conn.h>
#include <amp_tcp.h>
#include <amp_udp.h>

amp_proto_interface_t  *amp_protocol_interface_table[AMP_CONN_TYPE_MAX];


void amp_proto_interface_table_init ()
{

	memset(amp_protocol_interface_table, 
			0, 
			sizeof(amp_proto_interface_t *) * AMP_CONN_TYPE_MAX);

	/*
	 * add tcp
	 */ 
    amp_protocol_interface_table[AMP_CONN_TYPE_TCP] = &amp_tcp_proto_interface;
    amp_protocol_interface_table[AMP_CONN_TYPE_UDP] = &amp_udp_proto_interface;
}

/*end of file*/
