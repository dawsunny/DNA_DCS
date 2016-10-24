/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#include <amp_help.h>

//amp_u64_t  amp_debug_mask = AMP_DEBUG_ERROR | AMP_DEBUG_WARNING |AMP_DEBUG_ENTRY | AMP_DEBUG_LEAVE | AMP_DEBUG_MSG;
//amp_u64_t  amp_debug_mask = AMP_DEBUG_ERROR | AMP_DEBUG_WARNING |AMP_DEBUG_ENTRY | AMP_DEBUG_LEAVE;
amp_u64_t  amp_debug_mask = AMP_DEBUG_ERROR | AMP_DEBUG_WARNING;
//amp_u64_t amp_debug_mask = 0;

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


/*end of file*/
