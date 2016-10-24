/*header file of client_cnd */

#include "dcs_debug.h"
#include "amp.h"

extern amp_comp_context_t *clt_comp_context;
extern dcs_u32_t   clt_this_id;
extern dcs_u32_t   clt_num;
extern dcs_u32_t   clt_optype;
//extern dcs_u32_t   server_id;
extern dcs_s8_t    *clt_pathname;

//add by bxz
extern dcs_s32_t clt_filetype;

/*client parse paramatter */
dcs_s32_t __dcs_clt_parse_parameter(dcs_s32_t argc, dcs_s8_t **argv);
/* create connection bewteen client and server */
dcs_s32_t __dcs_clt_init_com(void);
/*backgroud server*/
inline int __dcs_daemonlize();
/*get server addr*/
dcs_s8_t *getaddr(dcs_u32_t server_id);
/*client alloc buf for reply iov*/
dcs_s32_t __client_allocbuf(void *msgp,
                              amp_u32_t *niov, amp_kiov_t **iov);
/*free iov buf*/
void __client_freebuf(amp_u32_t niov, amp_kiov_t *iov);
/*init send buf*/
/*
dcs_s32_t __client_init_sendbuf(dcs_u32_t size, 
                                amp_u32_t *niov, 
                                amp_kiov_t **iov, 
                                dcs_s8_t *buf)
*/
