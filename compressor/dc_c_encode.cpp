#include <string.h>

#include "dc_type.h"
#include "dc_const.h"

#include "dc_c_encode.h"

dc_u32_t
base_to_uint(dc_s8_t base)
{
    switch(base) 
    {
    case 'A':   return 0;
    case 'C':   return 1;
    case 'G':   return 2;
    case 'T':   return 3;
    default:    return -1; // case 'N'                                                                                                                              
    }
}

//half encode
dc_u8_t
char_to_halfByte(dc_s8_t ch) 
{
	if( '0' <= ch && ch <= '7') //integer
    {
		return ch - '0';
	}   

    switch(ch) 
    {  
    case 'A':   return  8;
    case 'C':   return  9;
    case 'G':   return 10; 
    case 'T':   return 11; 
    case '-':   return 12; 
    case 'i':   return 13; 
    case 's':   return 14; 
    default:    return 15; 
    }   
}    
