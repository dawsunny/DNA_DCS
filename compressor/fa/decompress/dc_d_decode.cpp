#include "dc_type.h"
#include "dc_d_decode.h"

dc_s8_t
halfByte_to_char(dc_u8_t ch) 
{
	if(0 <= ch && ch <= 7) //integer
    {
		return ch + '0';
	}   

    switch(ch) 
    {
    case  8:   return 'A'; 
    case  9:   return 'C'; 
    case 10:   return 'G'; 
    case 11:   return 'T'; 
    case 12:   return '-'; 
    case 13:   return 'i'; 
    case 14:   return 's'; 
    default:   return 'N'; 
    }   
}    
