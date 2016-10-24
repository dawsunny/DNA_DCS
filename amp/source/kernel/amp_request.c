/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef EXPORT_SYMTAB
#define EXPORT_SYMTAB
#endif

#include <amp_request.h>

struct list_head  amp_sending_list;
amp_lock_t        amp_sending_list_lock;

struct list_head  amp_waiting_reply_list;
amp_lock_t        amp_waiting_reply_list_lock;

amp_lock_t        amp_free_request_lock;

amp_u64_t         amp_xid_generator;
amp_lock_t         amp_xid_lock;


#ifdef __KERNEL__
struct kmem_cache  *amp_request_cache = NULL;
#endif

/*
 * initialization the fundation of request
 */ 
int
__amp_init_request(void)
{
	amp_s32_t err = 0;

	AMP_ENTER("__amp_init_request: enter\n");

	INIT_LIST_HEAD(&amp_sending_list);
	amp_lock_init(&amp_sending_list_lock);

	INIT_LIST_HEAD(&amp_waiting_reply_list);
	amp_lock_init(&amp_waiting_reply_list_lock);

	
	amp_lock_init(&amp_free_request_lock);


	amp_xid_generator = 1;
	amp_lock_init(&amp_xid_lock);



#ifdef __KERNEL__
	amp_request_cache = kmem_cache_create("amp_requests",
					  sizeof(amp_request_t),
					  0,
					  SLAB_HWCACHE_ALIGN, NULL, NULL);
	if (!amp_request_cache) {
		AMP_ERROR("__amp_init_request: create cache error\n");
		err = -ENOMEM;
		goto EXIT;
	}
#endif
EXIT:
	AMP_LEAVE("__amp_init_request: leave\n");
	return err;
}

/*
 * finalize
 */ 
int 
__amp_finalize_request (void)
{
	amp_s32_t  err = 0;

	AMP_ENTER("__amp_finalize_request: enter\n");
#ifdef __KERNEL__
	if (amp_request_cache)
		kmem_cache_destroy (amp_request_cache);
#endif

	AMP_LEAVE("__amp_finalize_request: leave\n");
	return err;
}

/*
 * alloc a request
 */ 
int
__amp_alloc_request (amp_request_t **retreq)
{
	amp_s32_t err = 0;
	amp_request_t *reqp = NULL;

	AMP_ENTER("__amp_alloc_request: enter\n");

AGAIN:
		

#ifdef __KERNEL__
	reqp = kmem_cache_alloc(amp_request_cache, GFP_KERNEL);
#else
	reqp = (amp_request_t *)malloc(sizeof(amp_request_t));
#endif

	if (!reqp) {
		AMP_WARNING("__amp_alloc_request: alloc request error\n");
		goto AGAIN;
	}

	/*
	 * initialization
	 */ 
	memset(reqp, 0, sizeof(amp_request_t));

	INIT_LIST_HEAD(&reqp->req_list);
	amp_sem_init_locked(&reqp->req_waitsem);
	amp_lock_init(&reqp->req_lock);
	reqp->req_state = AMP_REQ_STATE_INIT;
	reqp->req_iov = NULL;
	reqp->req_msg = NULL;
	reqp->req_reply = NULL;
	reqp->req_need_free = 0;
	atomic_set(&reqp->req_refcount, 1);  /*initialized to 1*/
	
	*retreq = reqp;

	AMP_LEAVE("__amp_alloc_request: leave, req:%p, refcount:%d\n", \
                   reqp, atomic_read(&reqp->req_refcount));
	return err;
}

/*
 * free a request
 */ 
int
__amp_free_request (amp_request_t *req)
{
	amp_s32_t err = 0;

	AMP_ENTER("__amp_free_request: enter, req:%p, refcount:%d\n", \
                   req, atomic_read(&req->req_refcount));

	if (!req) {
		AMP_ERROR("__amp_free_request: no request\n");
		err = -EINVAL;
		goto EXIT;
	}

	AMP_DMSG("__amp_free_request: lock free request lock\n");
	amp_lock(&amp_free_request_lock);

	AMP_DMSG("__amp_free_request: lock request\n");
	amp_lock(&req->req_lock);

	if (!atomic_dec_and_test(&req->req_refcount)) {
		amp_unlock(&req->req_lock);
		amp_unlock(&amp_free_request_lock);
		goto EXIT;
	}
	amp_unlock(&req->req_lock);
	memset(req, 0, sizeof(amp_request_t));
	AMP_DMSG("__amp_free_request: fully free req:%p\n", req);
	
#ifdef __KERNEL__
	kmem_cache_free(amp_request_cache, req);
#else
	free(req);
#endif
	amp_unlock(&amp_free_request_lock);

EXIT:
	AMP_LEAVE("__amp_free_request: leave\n");
	return err;
}

/*
 * change from handle to request, if match all the parameter, then return it
 */ 
/*
amp_request_t *
__amp_handle2req (amp_u64_t handle, 
		          amp_u64_t xid, 
				  amp_u32_t pid, 
				  amp_time_t sendts)
{
	amp_request_t *req = NULL;

	AMP_ENTER("__amp_handle2req: enter\n");

	amp_lock(&amp_free_request_lock);
	req = (amp_request_t *)((unsigned long)handle);

	if ((req->req_xid == xid) &&
			(req->req_pid == pid) &&
			(req->req_send_ts == sendts)) {
		amp_lock(req->req_lock);
		req->req_refcount ++;
		amp_unlock(req->req_lock);
	} else 
		req = NULL;
		
EXIT:
	amp_unlock(&amp_free_request_lock);
	AMP_LEAVE("__amp_handle2req: leave\n");
	return req;
}
*/
/*
 * get a new xid
 */
amp_u64_t   
__amp_getxid()
{
	amp_u64_t  newxid;

	amp_lock(&amp_xid_lock);
	newxid = amp_xid_generator++;
	amp_unlock(&amp_xid_lock);

	return newxid;
}

/*
 * judge whether two message header is equal or not.
 */
int  __amp_reqheader_equal(amp_message_t  *src,  amp_message_t *dst)
{
	amp_u32_t  equal = 0;

	AMP_ENTER("__amp_reqheader_equal: enter\n");
	if (src->amh_magic != dst->amh_magic)
		goto EXIT;
	
	if (src->amh_pid != dst->amh_pid)
		goto EXIT;
	
	if (src->amh_sender_handle != dst->amh_sender_handle)
		goto EXIT;
	
	if (src->amh_send_ts.sec != dst->amh_send_ts.sec)
		goto EXIT;
	
	if (src->amh_send_ts.usec != dst->amh_send_ts.usec)
		goto EXIT;

	if (src->amh_xid != dst->amh_xid)
		goto EXIT;

	equal = 1;	

EXIT:
	AMP_LEAVE("__amp_reqheader_equal: leave\n");

	return equal;
}

EXPORT_SYMBOL(__amp_alloc_request);
EXPORT_SYMBOL(__amp_free_request);
/*end of file*/
