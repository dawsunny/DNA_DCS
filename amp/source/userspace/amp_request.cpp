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
amp_u32_t         amp_seqno_generator;
amp_lock_t        amp_seqno_lock;

/**
 *get a new seqno of message
 */
amp_u32_t
__amp_get_seqno(void)
{
    amp_u32_t  newxid;
    amp_lock(&amp_seqno_lock);
    newxid = amp_seqno_generator++;
    amp_unlock(&amp_seqno_lock);
    return newxid;
}

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
    amp_lock_init(&amp_seqno_lock);
    amp_seqno_generator = 1;

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
        
    reqp = (amp_request_t *)malloc(sizeof(amp_request_t));

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
    reqp->req_refcount = 1;  /*initialized to 1*/
    
    *retreq = reqp;

    AMP_LEAVE("__amp_alloc_request: leave, req:%p\n", reqp);
    return err;
}


/*
 * free a request
 */ 
int
__amp_free_request (amp_request_t *req)
{
    amp_s32_t err = 0;

    AMP_ENTER("__amp_free_request: enter, req:%p, refcount:%d\n", req, req->req_refcount);
    if (!req) {
        AMP_ERROR("__amp_free_request: no request\n");
        err = -EINVAL;
        goto EXIT;
    }

    AMP_DMSG("__amp_free_request: lock free_request_lock\n");

    if(!list_empty(&req->req_list))
    {
        amp_lock(&amp_sending_list_lock);
        amp_lock(&req->req_lock);
        if(__amp_within_sending_list(req))
            list_del_init(&req->req_list);
        amp_unlock(&req->req_lock);
        amp_unlock(&amp_sending_list_lock);

        amp_lock(&amp_waiting_reply_list_lock);
        amp_lock(&req->req_lock);
        if(__amp_within_waiting_reply_list(req))
            list_del_init(&req->req_list);
        amp_unlock(&req->req_lock);
        amp_unlock(&amp_waiting_reply_list_lock);
                
        __amp_remove_resend_req(req);
    }

    amp_lock(&amp_free_request_lock);

    AMP_DMSG("__amp_free_request: lock req lock\n");
    amp_lock(&req->req_lock);
    req->req_refcount --;

    if (req->req_refcount > 0) {
        amp_unlock(&req->req_lock);
        amp_unlock(&amp_free_request_lock);
        goto EXIT;
    }
    amp_unlock(&req->req_lock);
    memset(req, 0, sizeof(amp_request_t));
    AMP_LEAVE("__amp_free_request: fully free req:%p\n", req);
    free(req);
    amp_unlock(&amp_free_request_lock);

EXIT:
    AMP_LEAVE("__amp_free_request: leave\n");
    return err;
}

/*
 * judge whether two message header is equal or not.
 */
int  __amp_reqheader_equal(amp_message_t  *src,  amp_message_t *dst)
{
    amp_u32_t  equal = 0;

    AMP_ENTER("__amp_reqheader_equal: enter\n");
    if (src->amh_seqno != dst->amh_seqno){
        AMP_WARNING("__amp_reqheader_equal: amh_seqno not equal, src:%u, dst:%u\n",src->amh_seqno,dst->amh_seqno);
        goto EXIT;
    }
    if (src->amh_send_ts.usec != dst->amh_send_ts.usec){
        AMP_WARNING("__amp_reqheader_equal: amh_send_ts.usec not equal, src:%lld, dst:%lld\n",src->amh_send_ts.usec,dst->amh_send_ts.usec);
        goto EXIT;
    }

    if (src->amh_send_ts.sec != dst->amh_send_ts.sec){
        AMP_WARNING("__amp_reqheader_equal: amh_send_ts.sec not equal, src:%lld, dst:%lld\n",src->amh_send_ts.sec,dst->amh_send_ts.sec);
        goto EXIT;
    }
    if (src->amh_magic != dst->amh_magic){
        AMP_WARNING("__amp_reqheader_equal: amh_magic not equal, src:%u, dst:%u\n",src->amh_magic,dst->amh_magic);
        goto EXIT;
    }
    if (src->amh_sender_handle != dst->amh_sender_handle){
        AMP_WARNING("__amp_reqheader_equal: amh_sender_handle not equal, src:%lld, dst:%lld\n",src->amh_sender_handle,dst->amh_sender_handle);
        goto EXIT;
    }
    if (src->amh_pid != dst->amh_pid){
        AMP_WARNING("__amp_reqheader_equal: amh_pid not equal, src:%u, dst:%u\n",src->amh_pid,dst->amh_pid);
        goto EXIT;
    }
    if (src->amh_xid != dst->amh_xid){
        AMP_WARNING("__amp_reqheader_equal: amh_xid not equal, src:%u, dst:%u\n",src->amh_xid,dst->amh_xid);
        goto EXIT;
    }

    equal = 1;  

EXIT:
    AMP_LEAVE("__amp_reqheader_equal: leave\n");

    return equal;
}

/*end of file*/
