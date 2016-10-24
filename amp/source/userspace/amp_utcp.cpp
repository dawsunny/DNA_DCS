/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */
/*          Rongfeng Tang                      */
/***********************************************/
#include <amp_tcp.h>
#include <amp_thread.h>

static inline int 
__amp_tcp_general_init (amp_s32_t sock)
{
    amp_s32_t err = 0;
    amp_s32_t rc = 0;
    struct timeval tv;
    int option;
    struct linger linger;

    AMP_ENTER("__amp_tcp_general_init: enter\n");

    /*
     * set recv and send timeout
     */ 
    tv.tv_sec = AMP_ETHER_SNDTIMEO;
    tv.tv_usec = 0;

    rc = setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (amp_s8_t*)&tv, sizeof(tv));
    if (rc) {
        AMP_ERROR("__amp_tcp_general_init: set sndtimeo error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }

    tv.tv_sec = AMP_ETHER_RCVTIMEO;

    rc = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (amp_s8_t *)&tv, sizeof(tv));
    if (rc) { 
        AMP_ERROR("__amp_tcp_general_init: set rcvtimeout error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }

    
    /*
     * close nangle
     */ 
    option = 1;
    rc = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (amp_s8_t *)&option, sizeof(option));
    if (rc) {
        AMP_ERROR("__amp_tcp_general_init: close nangle error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }

    /*
     * set send and rcv buffer size
     */ 
    option = AMP_ETHER_SNDBUF;
    rc = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (amp_s8_t *)&option, sizeof(option));
    if (rc) {
        AMP_ERROR("__amp_tcp_general_init: set sndbuf error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }

    option = AMP_ETHER_RCVBUF;
    rc = setsockopt(sock, SOL_SOCKET, SO_RCVBUF,(amp_s8_t *)&option, sizeof(option));
    if (rc) {
        AMP_ERROR("__amp_tcp_general_init: set rcvbuf error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }

    /*
     * close linger
     */ 
    linger.l_onoff = 1;
    linger.l_linger = 0;

    rc = setsockopt(sock, SOL_SOCKET, SO_LINGER, (amp_s8_t *)&linger, sizeof(linger));
    if (rc) {
        AMP_ERROR("__amp_tcp_general_init: close linger error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }

    option = -1;
    rc = setsockopt(sock, SOL_TCP, TCP_LINGER2, (amp_s8_t *)&option,
           sizeof(option));
    if (rc) {
        AMP_ERROR("__amp_tcp_general_init: close linger2 error, rc:%d\n", errno);
        err = -errno;
        goto EXIT;
    }
    
    option = 1;
    rc = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (char *)&option, sizeof(option));
    if (rc < 0) {
        AMP_ERROR("__amp_tcp_general_init: set keepalive error, rc:%d, errno:%d\n", rc, errno);
        err = -errno;
        goto EXIT;
    }

    option = AMP_KEEP_IDLE;
    rc = setsockopt(sock, SOL_TCP, TCP_KEEPIDLE, (char *)&option, sizeof(option));
    if (rc < 0) {
        AMP_ERROR("__amp_tcp_general_init: set keep idle error, rc:%d, errno:%d\n", rc, errno);
        err = -errno;
        goto EXIT;
    }

    option = AMP_KEEP_INTVL;
    rc = setsockopt(sock, SOL_TCP, TCP_KEEPINTVL, (char *)&option, sizeof(option));
    if (rc < 0) {
        AMP_ERROR("__amp_tcp_general_init: set keep intvl error,rc:%d,errno:%d\n", rc, errno);
        err = -errno;
        goto EXIT;
    }

    option = AMP_KEEP_COUNT;
    rc = setsockopt(sock, SOL_TCP, TCP_KEEPCNT, (char *)&option, sizeof(option));
    if (rc < 0) {
        AMP_ERROR("__amp_tcp_general_init: set keep count error,rc:%d,errno:%d\n", rc, errno);
        err = -errno;
        goto EXIT;
    }
EXIT:
    AMP_LEAVE("__amp_tcp_general_init: leave\n");
    return err;
}

/* 
 * send data to peer by tcp protocol.
 * return: 0 - normal, <0 - abnormal
 */ 
int
__amp_tcp_sendmsg (void *protodata,
                   void *addr,
                   amp_u32_t addr_len,
                   amp_u32_t len, 
                   void *bufp,
                   amp_u32_t flags)
{
    amp_s32_t err = 0;
    amp_s32_t rc = 0;
    amp_s32_t sock = -1;
    amp_s32_t  slen;
    amp_s8_t  *tmpbufp = NULL;
    amp_u32_t sleep_time = 0;    
    amp_u32_t sendlen;

    AMP_ENTER("__amp_tcp_sendmsg enter, len:%d\n", len);

    sock = *((amp_u32_t *)protodata);

    if (sock < 0) {
        AMP_ERROR("__amp_tcp_sendmsg: no sock\n");
        err = -EINVAL;
        goto EXIT;
    }

    if (!bufp || !len) {
        AMP_ERROR("__amp_tcp_sendmsg: no buffer\n");
        err = -EINVAL;
        goto EXIT;
    }

    tmpbufp = (amp_s8_t *)bufp;
    slen = len;

    while (slen) {
        sleep_time = 0;
RE_WRITE:
        sendlen = (slen < 4096)?(slen):(4096);
        rc = write(sock, tmpbufp, sendlen);
        if (rc <= 0) {
            AMP_ERROR("__amp_tcp_sendmsg: send msg error, errno:%d\n", errno);
            if (errno == EAGAIN || errno == EINTR){
                sleep_time ++;
                usleep(1000);
                if(sleep_time < AMP_CONN_RETRY_TIMES)
                    goto RE_WRITE;
            }
            if (rc == 0)
                rc = -ECONNABORTED;

            err = -errno;
            goto EXIT;
        }
        if (rc < slen) 
            AMP_DMSG("__amp_tcp_sendmsg: sent:%c, slen:%d, less\n", rc, slen);

        tmpbufp += rc;
        slen -= rc;
    }
EXIT:
    AMP_LEAVE("__amp_tcp_sendmsg: leave\n");
    return err;
}

/*
 * send data to peer by tcp protocol.
 */
int 
__amp_tcp_senddata(void *protodata, 
                   void *addr,
                   amp_u32_t addr_len,
                   amp_u32_t niov,
                   amp_kiov_t *iov,
                   amp_u32_t flags)
{
    amp_s32_t err = 0;
    amp_s32_t rc = 0;
    amp_s32_t sock;
    amp_u32_t sleep_time = 0;
    amp_s8_t  *addrp = NULL;
    amp_kiov_t *kiov;
    amp_u32_t slen;
    amp_u32_t sendlen;
    amp_u32_t iovs;
    char *bufp;

    AMP_ENTER("__amp_tcp_senddata: enter\n");
    sock = *((amp_s32_t *)protodata);

    if (sock < 0) {
        AMP_ERROR("__amp_tcp_senddata: no sock\n");
        err = EINVAL;
        goto EXIT;
    }

    if(!iov || !niov) {
        AMP_ERROR("__amp_tcp_senddata: no iov\n");
        err = -EINVAL;
        goto EXIT;
    }

    iovs = niov;
    kiov = iov;

    AMP_DMSG("__amp_tcp_senddata: niov:%d, iov:%p\n", iovs, kiov);
    while (iovs) {
        addrp = (amp_s8_t *)kiov->ak_addr;
        if (!addrp) {
            AMP_ERROR("__amp_tcp_senddata: no buffer\n");
            AMP_ERROR("__amp_tcp_senddata: total iov:%d, current ios:%d\n",niov, iovs);
            err = -EINVAL;
            goto EXIT;
        }
        bufp = addrp;
        bufp +=  kiov->ak_offset;
        slen = kiov->ak_len;
        
        AMP_ENTER("__amp_tcp_senddata: addrp:%p, slen:%d\n", addrp, slen);
        
        while (slen) {
            sleep_time = 0;
RE_WRITE:
            sendlen = (slen < 4096)?(slen):(4096);
            rc = write(sock, bufp, sendlen);
            if (rc <= 0) {
                AMP_ERROR("__amp_tcp_senddata: send error, errno:%d\n", errno);
                if (errno == EAGAIN || errno == EINTR){
                    sleep_time ++;
                    usleep(1000);
                    if(sleep_time < AMP_CONN_RETRY_TIMES)
                        goto RE_WRITE;
                }
                if (rc == 0)
                    rc = -ECONNABORTED;
                err = -errno;
                goto EXIT;
            }
            if (rc < slen) 
                AMP_DMSG("__amp_tcp_senddata: sent:%d, slen:%d, less\n", rc, slen);
            AMP_DMSG("__amp_tcp_sendata: total sent:%d\n", rc);

            bufp += rc;
            slen -= rc;
        }

        iovs --;
        kiov ++;
    }

EXIT:
    AMP_LEAVE("__amp_tcp_senddata: leave\n");
    return err;
}

/* 
 * recvmsg
 */ 
int 
__amp_tcp_recvmsg(void *protodata,
                  void *addr,
                  amp_u32_t addr_len,
                  amp_u32_t len,
                  void *bufp,
                  amp_u32_t flags)
{
    amp_s32_t err = 0;
    amp_s32_t rc = 0;
    amp_u32_t slen;
    amp_s32_t sock;
    char  *tmpbufp = NULL;
    amp_u32_t sleep_time = 0;
    amp_u32_t readlen;
    AMP_ENTER("__amp_tcp_recvmsg: enter, len:%d\n", len);

    sock = *((amp_s32_t *)protodata);
    
    if (sock < 0) {
        AMP_ERROR("__amp_tcp_recvmsg: no socket\n");
        err = -EINVAL;
        goto EXIT;
    }
    
    AMP_DMSG("__amp_tcp_rcvmsg: sock:%d\n", sock);

    if (!bufp || !len) {
        AMP_ERROR("__amp_tcp_recvmsg: no buffer\n");
        err = -EINVAL;
        goto EXIT;
    }

    tmpbufp = (amp_s8_t *)bufp;

    slen = len;
 
    sleep_time = 0;
    while (slen) {
        sleep_time = 0;
RE_READ:
        readlen = (slen < 4096)?(slen):(4096);
        rc = read (sock, tmpbufp, readlen);
        if (rc <= 0) {
            if (errno == EAGAIN || errno == EINTR){
                sleep_time ++;
                usleep(1000);
                if(sleep_time < AMP_CONN_RETRY_TIMES)
                    goto RE_READ;
            }
            AMP_ERROR("__amp_tcp_recvmsg: recv error, sock:%d, errno:%d\n", sock, errno);
            AMP_ERROR("__amp_tcp_recvmsg: recv error, tmpbufp:%p, slen:%d\n",tmpbufp, slen);

            err = -errno;
            if (rc == 0)
                err = -ECONNABORTED;
            goto EXIT;
        }

        AMP_DMSG("__amp_tcp_recvmsg: returned rc:%d\n", rc);

        tmpbufp += rc;
        slen -= rc;
    }
    
    err = 0;

EXIT:
    AMP_LEAVE("__amp_tcp_recvmsg: leave\n");
    return err;

}


/*
 * recv data blocks
 */ 
int 
__amp_tcp_recvdata (void *protodata,
                    void *addr,
                    amp_u32_t addr_len,
                    amp_u32_t niov,
                    amp_kiov_t *iov,
                    amp_u32_t flags)
{
    amp_s32_t err = 0;
    amp_s32_t rc = 0;
    amp_u32_t slen;
    amp_u32_t iovs;
    amp_s32_t sock;
    amp_u32_t sleep_time = 0;
    amp_kiov_t *kiov = NULL;
    char *bufp = NULL;
    amp_u32_t readlen;
    AMP_ENTER("_amp_tcp_recvdata: enter\n");

    sock = *((amp_s32_t *)protodata);

    if (sock < 0) {
        AMP_ERROR("__amp_tcp_recvdata: no sock\n");
        err = -EINVAL;
        goto EXIT;
    }

    if (!iov || !niov) {
        AMP_ERROR("__amp_tcp_recvdata: no kiov\n");
        err = -EINVAL;
        goto EXIT;
    }

    AMP_DMSG("__amp_tcp_recvdata: niov:%d, iov:%p\n", niov, iov);

    kiov = iov;
    iovs = niov;

    while (iovs) {
        bufp = (amp_s8_t *) kiov->ak_addr;
        bufp += kiov->ak_offset;
        slen = kiov->ak_len;

        while (slen) {
            sleep_time = 0;
RE_READ:
            readlen = (slen < 4096)?(slen):(4096);
            rc = read (sock, bufp, readlen);
            if (rc <= 0) {
                if (errno == EAGAIN||errno == EINTR) {
                    sleep_time ++;
                    usleep(1000);
                    if(sleep_time < AMP_CONN_RETRY_TIMES)
                        goto RE_READ;
                }

                AMP_ERROR("__amp_tcp_recvdata: recv error, rc:%d\n", errno);
                if(rc == 0)
                    err = -ECONNABORTED;
                else
                    err = -errno;
                goto EXIT;
            }
            bufp += rc;
            slen -= rc;
        }

        iovs --;
        kiov ++;
    }
EXIT:
    AMP_LEAVE("__amp_tcp_recvdata: leave\n");
    return err;
}

/*
 * doing a connect
 */ 
int 
__amp_tcp_connect (void *protodata_parent,
                   void **protodata_child,
                   void *addr,
                   amp_u32_t direction)
{
    amp_s32_t err;
    amp_s32_t  parent_sock, *new_sock = NULL;
    struct sockaddr_in *saddrp;
    //amp_s32_t  opt = 1;
    struct sockaddr_in remoteaddr;
    amp_u32_t addrlen;

    AMP_ENTER("__amp_tcp_connect: enter\n");
    switch (direction) {
        case AMP_CONN_DIRECTION_LISTEN:
            AMP_DEBUG(AMP_DEBUG_TCP|AMP_DEBUG_MSG, "__amp_tcp_connect: listen\n");
            if (!addr) {
                AMP_ERROR("__amp_tcp_connect: no address\n");
                err = -EINVAL;
                goto EXIT;
            }
            new_sock = (amp_s32_t *)amp_alloc(sizeof(amp_s32_t));
            *new_sock = -1;
            saddrp = (struct sockaddr_in *)addr;

            err = socket(AF_INET, SOCK_STREAM, 0);
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: create socket error, err:%d\n", errno);
                err = -errno;
                goto EXIT;
            }
            *new_sock = err;
            /*err = setsockopt(*new_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(amp_s32_t));
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: setopt error,err:%d\n", errno);
                close(*new_sock);
                goto EXIT;
            }*/
            err = bind(*new_sock, (struct sockaddr *)saddrp, sizeof(*saddrp));
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: bind error, err:%d\n", errno);
                close(*new_sock);
                goto EXIT;
            }

            err = listen(*new_sock, 65536);
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: listen error, err:%d\n", errno);
                close(*new_sock);
                goto EXIT;
            }
            break;

        case AMP_CONN_DIRECTION_ACCEPT:
            AMP_DEBUG(AMP_DEBUG_TCP|AMP_DEBUG_MSG, "__amp_tcp_connect: accept\n");
            if (!protodata_parent) {
                AMP_ERROR("__amp_tcp_connect: no parent socket\n");
                err = -EINVAL;
                goto EXIT;
            }
            parent_sock = *((amp_s32_t *)protodata_parent);
            bzero(&remoteaddr, sizeof(remoteaddr));
            addrlen = sizeof(remoteaddr);
            new_sock = (amp_s32_t *)amp_alloc(sizeof(amp_s32_t));
            err = accept(parent_sock, (struct sockaddr *)&remoteaddr, &addrlen);
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: accept error, err:%d\n", errno);
                goto EXIT;
            }
            *new_sock = err;
            break;
        case AMP_CONN_DIRECTION_CONNECT:
            AMP_DEBUG(AMP_DEBUG_TCP|AMP_DEBUG_MSG, "__amp_tcp_connect: connect\n");
            if (!addr) {
                AMP_ERROR("__amp_tcp_connect: no address\n");
                err = -EINVAL;
                goto EXIT;
            }
            saddrp = (struct sockaddr_in *)addr;
            err = socket(AF_INET, SOCK_STREAM, 0);
            
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: create socket error, err:%d\n", errno);
                goto EXIT;
            }
            new_sock = (amp_s32_t *)amp_alloc(sizeof(amp_s32_t));
            *new_sock = err;
            /*err = setsockopt(*new_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(amp_s32_t));
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: setopt error,err:%d\n", errno);
                close(*new_sock);
                goto EXIT;
            }*/

            err = connect(*new_sock, (struct sockaddr *)saddrp, sizeof(*saddrp));
            if (err < 0) {
                AMP_ERROR("__amp_tcp_connect: connect to server error, err:%d\n", errno);
                close(*new_sock);
                goto EXIT;
            }
            
            break;
        default:
            AMP_ERROR("__amp_tcp_connect: wrong direction:%d\n", direction);
            err = -EINVAL;
            break;
    }

    /*
     * now doing some initialization
     */

    err = __amp_tcp_general_init(*new_sock);
    if (err) {
        close(*new_sock);
        goto EXIT;
    }

    /*
     * return it to connection
     */ 
    *protodata_child = new_sock;

EXIT:
    if(err < 0)
        free(new_sock);
    AMP_LEAVE("__amp_tcp_connect: leave\n");
    return err;
}

/*
 * disconnect 
 */ 

int
__amp_tcp_disconnect (void *protodata)
{
    amp_s32_t err = 0;
    amp_s32_t  sock;
    AMP_ENTER("__amp_tcp_disconnect: enter\n");

    if (!protodata) {
        AMP_ERROR("__amp_tcp_disconnect: no sock\n");
        err = -EINVAL;
        goto EXIT;
    }

    sock = *((amp_s32_t *)protodata);
    AMP_DMSG("__amp_tcp_disconnect: get sock:%d\n", sock);
    shutdown(sock,SHUT_RDWR);
    close(sock);
EXIT:
    AMP_LEAVE("__amp_tcp_disconnect: leave\n");
    return err;
}

/*
 * init the  socket
 */
int 
__amp_tcp_init (void * protodata, amp_u32_t direction)
{
    amp_s32_t  err = 0;
    amp_s32_t sock;
    
    AMP_ENTER("__amp_tcp_init: enter\n");

    if (!protodata) {
        AMP_ERROR("__amp_tcp_init: no proto data\n");
        err = -EINVAL;
        goto EXIT;
    }

    sock = *((amp_s32_t *)protodata);

EXIT:
    AMP_LEAVE("__amp_tcp_init: leave\n");
    return err;
}


amp_proto_interface_t  amp_tcp_proto_interface = {
    type:                  AMP_CONN_TYPE_TCP,
    amp_proto_sendmsg:     __amp_tcp_sendmsg,
    amp_proto_senddata:    __amp_tcp_senddata,
    amp_proto_recvmsg:     __amp_tcp_recvmsg,
    amp_proto_recvdata:    __amp_tcp_recvdata,
    amp_proto_connect:     __amp_tcp_connect,
    amp_proto_disconnect:  __amp_tcp_disconnect,
    amp_proto_init:        __amp_tcp_init
};



/*end of file*/
