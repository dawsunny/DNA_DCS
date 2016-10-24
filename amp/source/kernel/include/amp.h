/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_H_
#define __AMP_H_

#include <amp_sys.h>
#include <amp_types.h>
#include <amp_request.h>
#include <amp_conn.h>
#include <amp_help.h>
#include <amp_list.h>
#include <amp_thread.h>
#include <amp_protos.h>
#include <amp_ioctl.h>


/*external functions*/
extern int  amp_create_connection (amp_comp_context_t *ctxt,
			    amp_u32_t remote_type,
			    amp_u32_t remote_id,
			    amp_u32_t addr,
			    amp_u32_t port,
		 	    amp_u32_t conn_type,
		   	    amp_u32_t direction,
		 	    int (*queue_req) (amp_request_t *req),
			    int (*allocpages) (void *, amp_u32_t *, amp_kiov_t **),
			    void (*freepages)(amp_u32_t , amp_kiov_t *));

int amp_send_sync (amp_comp_context_t *,
		  amp_request_t *,
	          amp_u32_t ,
	          amp_u32_t,
	          amp_s32_t);

int amp_send_async (amp_comp_context_t *,
		    amp_request_t *,
		    amp_u32_t ,
		    amp_u32_t,
		    amp_s32_t);

amp_comp_context_t *amp_sys_init (amp_u32_t  type, amp_u32_t id);
int amp_disconnect_peer(amp_comp_context_t *ctxt, 
                        amp_u32_t remote_type, 
                        amp_u32_t remote_id, 
                        amp_u32_t forall);

int amp_sys_finalize (amp_comp_context_t *cmp_ctxt);

int amp_config (amp_s32_t cmd, void *conf, amp_s32_t len);

extern int __amp_alloc_request(amp_request_t **);
extern int __amp_free_request(amp_request_t *);
#endif
/*end of file*/
