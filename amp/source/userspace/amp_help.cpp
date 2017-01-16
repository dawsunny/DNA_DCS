/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#include <amp_help.h>
#include<amp_conn.h>
#include<amp_request.h>
#include<amp_protos.h>
#include<amp_thread.h>

//amp_u64_t  amp_debug_mask = AMP_DEBUG_ERROR | AMP_DEBUG_WARNING |AMP_DEBUG_ENTRY | AMP_DEBUG_LEAVE | AMP_DEBUG_MSG;
//amp_u64_t  amp_debug_mask = AMP_DEBUG_ERROR | AMP_DEBUG_WARNING |AMP_DEBUG_ENTRY | AMP_DEBUG_LEAVE;
amp_u64_t  amp_debug_mask = AMP_DEBUG_ERROR | AMP_DEBUG_WARNING;
//amp_u64_t  amp_debug_mask = 0;

/*
 * totally reset the debug mask 
 */ 
void 
amp_reset_debug_mask (amp_u64_t newmask)
{
    amp_debug_mask = newmask;
}

void 
amp_add_debug_bits (amp_u64_t mask_bits) 
{
    amp_debug_mask |= mask_bits;
}

void 
amp_clear_debug_bits (amp_u64_t mask_bits)
{
    amp_debug_mask &= (~mask_bits);
}

/*
 *amp_sem_down2 belong to req's req->req_waitsem
 */

int
amp_sem_down2(amp_sem_t *req_waitsem)
{
    struct timespec ts;
    amp_request_t *req = NULL;
    amp_connection_t *conn = NULL;
    amp_u32_t err = 0;
    amp_u32_t conn_num = 0;
    amp_u32_t circle_num = 0;
    amp_u32_t time_wait = AMP_CONN_TIMEWAIT_INTERVAL;
TIMEWAIT:
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += time_wait;
    err = sem_timedwait(req_waitsem, &ts);
    req = list_entry(req_waitsem, amp_request_t, req_waitsem);
    conn = req->req_conn;
    
    if(circle_num > AMP_CONN_RETRY_TIMES){
        req->req_error = -ENETUNREACH;
        err = -ENETUNREACH;
        goto EXIT;
    }
    
    if(err && ETIMEDOUT == errno){
        circle_num++;
        time_wait += (AMP_CONN_TIMEWAIT_INTERVAL/2);
        conn_num = req->req_ctxt->acc_conns[req->req_remote_type].acc_remote_conns[req->req_remote_id].active_conn_num;
        
        if(conn && conn->ac_state == AMP_CONN_OK){
            AMP_ERROR("amp_sem_down_with_timeout_process: req send timeout...\n");
        
            if(circle_num == 1)
                goto RESEND;

            amp_lock(&conn->ac_lock);
            if(conn->ac_need_reconn)
                conn->ac_state = AMP_CONN_RECOVER;
            else
                conn->ac_state = AMP_CONN_CLOSE;
            amp_unlock(&conn->ac_lock);
            
            amp_lock(&req->req_lock);
            req->req_conn = NULL;
            amp_unlock(&req->req_lock);

            AMP_OP(conn->ac_type, proto_disconnect)((void *)&conn->ac_sock);
            
            amp_lock(&amp_reconn_conn_list_lock);
            amp_lock(&conn->ac_lock);
            if(list_empty(&conn->ac_reconn_list))
                list_add_tail(&conn->ac_reconn_list,&amp_reconn_conn_list);
            amp_unlock(&conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);
            amp_sem_up(&amp_reconn_sem);
        }else if(conn_num == -1){
            if(!list_empty(&req->req_list))
                list_del_init(&req->req_list);
            req->req_error = -ENOTCONN;
            err = -ENOTCONN;
            amp_sem_up(req_waitsem);
            goto EXIT;
        }else if(conn == NULL)
            goto RESEND;

        goto TIMEWAIT;
    }else if(err && EINTR == errno){
        goto TIMEWAIT;
    }else if(err == 0 && req->req_error == -ENETUNREACH){
        goto RESEND;
    }

EXIT:
    return err;

RESEND:
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
           
    amp_lock(&amp_sending_list_lock);
    amp_lock(&req->req_lock);
    if(list_empty(&req->req_list))
        list_add_tail(&req->req_list, &amp_sending_list);
    amp_unlock(&req->req_lock);
    amp_unlock(&amp_sending_list_lock);
    amp_sem_up(&amp_process_out_sem);
    goto TIMEWAIT;
}

/*end of file*/
