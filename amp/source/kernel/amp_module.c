/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __KERNEL__
#define __KERNEL__
#endif

#ifndef MODULE
#define MODULE
#endif

#ifndef EXPORT_SYMTAB
#define EXPORT_SYMTAB
#endif

#include "amp.h"

#if (LINUX_VERSION_CODE < KERNEL_VERSION(2,6,0))

#define AMP_MODULE_USE    MOD_INC_USE_COUNT
#define AMP_MODULE_UNUSE  MOD_DEC_USE_COUNT

#else

#define AMP_MODULE_USE    try_module_get(THIS_MODULE)
#define AMP_MODULE_UNUSE  module_put(THIS_MODULE)

#endif

static int ampdev_ioctl (struct inode *inode, 
						 struct file *file,
						 unsigned int cmd, 
						 unsigned long arg)
{
	char  buf[512];
	int   err = 0;

	if (current->fsuid != 0) {
		err = -EACCES;
		goto OUT;
	}

	if (_IOC_TYPE(cmd) != IOC_AMP_TYPE ||
		_IOC_NR(cmd) < IOC_AMP_MIN_NR ||
		_IOC_NR(cmd) > IOC_AMP_MAX_NR) {
		err = -EINVAL;
		goto OUT;
	}

	memset(buf, 0, 512);

OUT:
	return err; 
}

static int ampdev_open (struct inode *inode, 
		                struct file *file)
{
	AMP_MODULE_USE;
	return 0;
}

static int ampdev_release (struct inode *inode, 
		                   struct file *file)
{
	AMP_MODULE_UNUSE;
	return 0;
}

#define AMP_MINOR  (127)
static struct file_operations ampdev_fops = {
	ioctl: ampdev_ioctl,
	open:  ampdev_open,
	release: ampdev_release
};
static struct miscdevice amp_dev = {
	minor: AMP_MINOR,
	name: "amp_dev",
	fops: &ampdev_fops,
};



int 
amp_module_init (void)
{
	amp_s32_t  err = 0;

	AMP_ENTER("amp_module_init: enter\n");

	err = __amp_init_conn();
	if (err < 0) 
		goto EXIT;

	err = __amp_init_request();
	if (err < 0)  
		goto INIT_REQUEST_ERROR;

	amp_proto_interface_table_init ();

	err = __amp_threads_init ();
	if (err < 0)
		goto INIT_THREAD_ERROR;

EXIT:
	AMP_LEAVE("amp_module_init: leave\n");
	return err;
	
INIT_THREAD_ERROR:
	__amp_finalize_request();
	
INIT_REQUEST_ERROR:
	__amp_finalize_conn();
	goto EXIT;
}

void
amp_module_fini(void)
{
	AMP_WARNING("amp_module_fini: enter\n");
	
	__amp_threads_finalize ();
	__amp_finalize_conn ();
	__amp_finalize_request ();

	AMP_LEAVE("amp_module_fini: leave\n");
}

MODULE_AUTHOR("Rongfeng Tang");
MODULE_DESCRIPTION("Async Message Passing System");
MODULE_LICENSE("GPL");

module_init(amp_module_init);
module_exit(amp_module_fini);
/*end of file*/
