#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20
#define NUM_OF_RECORDS ((BF_BLOCK_SIZE-2*sizeof(int))/sizeof(Record))  //Number of records per Block.

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

//Declaration of a global array (for opening files).
Fileindex array[MAX_OPEN_FILES]; 

HT_ErrorCode HT_Init() {
  //insert code here

  for(int i=0;i<MAX_OPEN_FILES;i++)
  {
    array[i].flag=0;
    array[i].filedesc=0;
    array[i].buckets=0;
    strcpy(array[i].filename,"0");
  }
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
  //insert code here
  //Create a file with name filename.
  CALL_BF(BF_CreateFile(filename));

  //Open the file with name filename.
  int file_desc, zero=0;
  CALL_BF(BF_OpenFile(filename,&file_desc));

  //I will write to the first block of the file to make it a HASH_FILE.
  BF_Block* block;
  BF_Block_Init(&block);
  
  CALL_BF(BF_AllocateBlock(file_desc,block)); //Allocate memory for the first block I created.
  
  char* data;
  data=BF_Block_GetData(block); //Take the data of the first block.
  int integer=200;
  memcpy(data,&integer,sizeof(int)); //I choose a random intenger to show that this is a HASH_FILE.
  memcpy(data+sizeof(int),&buckets,sizeof(int));
  BF_Block_SetDirty(block); //Because we want to change the data of the first block.
  CALL_BF(BF_UnpinBlock(block));   //I unpin the block because I don't want it anymore.
  
  //Allocate memory for the buckets.
  int num;
  num=(buckets*sizeof(int))/BF_BLOCK_SIZE+1; 
  
  for(int i=0;i<num;i++)
  {
    CALL_BF(BF_AllocateBlock(file_desc, block));
    CALL_BF(BF_UnpinBlock(block));
  }

  //Close the file.
  CALL_BF(BF_CloseFile(file_desc));
  BF_Block_Destroy(&block);

  return HT_OK;
}

int Position_In_Array()
{
  for(int i=0;i<MAX_OPEN_FILES;i++)
  {
    if(array[i].flag==0)
    {
      return i;  
    }
  }
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  //insert code here
  //Open the file with name filename
  int file_desc;
  CALL_BF(BF_OpenFile(fileName,&file_desc));
  
  //Store filename,filedesc into the global array of opening files, and indexDesc.
  int q = Position_In_Array();
  //printf("%d\n",q);
  array[q].flag=1;
  array[q].filedesc=file_desc;
  strcpy(array[q].filename,fileName);
  *indexDesc=q;
  
  BF_Block* block;
  BF_Block_Init(&block);
  int integer,num_of_buckets;
  CALL_BF(BF_GetBlock(file_desc,0,block));
  char* data=BF_Block_GetData(block);
  memcpy(&integer,data,sizeof(int));
  if(integer!=200)  return HT_ERROR;
  //Store buckets into the global array of opening files (into the specific position).
  memcpy(&num_of_buckets,data+sizeof(int),sizeof(int));
  array[q].buckets=num_of_buckets;
  
  CALL_BF(BF_UnpinBlock(block));
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //Close the file with a specific identical number
  CALL_BF(BF_CloseFile(array[indexDesc].filedesc));
  
  array[indexDesc].flag=0;
  array[indexDesc].filedesc=0;
  array[indexDesc].buckets=0;
  char* c="0";
  strcpy(array[indexDesc].filename,c);
  
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {

  int num_of_block, hash, temp;
  int file_desc=array[indexDesc].filedesc;
  int buckets=array[indexDesc].buckets; 
  int eight_bytes=2*sizeof(int);  //4 bytes for count_records, 4 bytes for next_block.
  
  BF_Block* block;
  BF_Block_Init(&block);
  /*
  !!!!!!This has been replaced by array[indexdesc].buckets!!!!!!!!
  
  CALL_BF(BF_GetBlock(file_desc,0,block));  
  char* data=BF_Block_GetData(block);
  memcpy(&buckets,data+sizeof(int),sizeof(int));  //Take the number of buckets.
  CALL_BF(BF_UnpinBlock(block));
  */

  hash=record.id%buckets;  //Hash function.
  num_of_block=(hash*sizeof(int))/BF_BLOCK_SIZE+1;  //Find in which block is the willing bucket.
  CALL_BF(BF_GetBlock(file_desc,num_of_block,block));
  char* data=BF_Block_GetData(block);
  memcpy(&temp,data+hash*sizeof(int),sizeof(int));  //Take the key of the bucket.
  
  //If it is the first time that we visit this bucket.
  if(temp==0) 
  {
    int blocks_num;
    CALL_BF(BF_GetBlockCounter(file_desc,&blocks_num));
    //printf("First time %d \n",blocks_num);
    //blocks_num; //prosoxh
    memcpy(data+hash*sizeof(int),&blocks_num,sizeof(int));
    BF_Block_SetDirty(block); //prosoxh      

    CALL_BF(BF_UnpinBlock(block));  //prosoxh

    CALL_BF(BF_AllocateBlock(file_desc,block));
    ////////////
    //CALL_BF(BF_GetBlockCounter(file_desc,&blocks_num));
    //printf("First time2 %d \n",blocks_num);
    /////////////////
    data=BF_Block_GetData(block);
    int count_record=1;
    memcpy(data,&count_record,sizeof(int));
    memcpy(data+eight_bytes,&record,sizeof(Record));  //First 8 bytes are for count_record and next_block, others are for records.
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }
  else  //We have already visited this bucket at least once. 
  {
    CALL_BF(BF_UnpinBlock(block)); //prosoxh.
    int count_record;
    //printf("BLOCK %d\n",temp);
    while(1)
    {
      CALL_BF(BF_GetBlock(file_desc,temp,block));
      data=BF_Block_GetData(block);
      memcpy(&temp,data+sizeof(int),sizeof(int));
      //printf("NEXT BLOCK %d \n",temp);
      if(temp!=0)
      {  
        CALL_BF(BF_UnpinBlock(block)); 
      }
      else  
        break;
    }
    memcpy(&count_record,data,sizeof(int));
    //printf("RECORD %d \n", record.id);
    if(count_record<NUM_OF_RECORDS)  //Case 1: If there is freespace for the record.
    {
      //printf("FREESPACE %d\n",count_record);
      memcpy(data+eight_bytes+count_record*sizeof(Record),&record,sizeof(Record));
      count_record++;
      memcpy(data,&count_record,sizeof(int));
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block)); 
    }
    else  //Case 2: If there isn't any freespace for new Records, allocate a new block for this record.
    {
      //printf("NOT FREESPACE %d\n",count_record);
      int next_block, count_record=1;
      CALL_BF(BF_GetBlockCounter(file_desc,&next_block));
      //printf("next_block %d\n",next_block);  //prosoxH
      //next_block--;  //prosoxh.
      memcpy(data+sizeof(int),&next_block,sizeof(int));
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block)); //prosoxh.
      CALL_BF(BF_AllocateBlock(file_desc,block));
      data=BF_Block_GetData(block);
      memcpy(data,&count_record,sizeof(int));
      memcpy(data+eight_bytes,&record,sizeof(Record));
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
    }
  }
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {

  int num_of_block, hash, temp; 
  int file_desc=array[indexDesc].filedesc;
  int buckets=array[indexDesc].buckets;
  int eight_bytes=2*sizeof(int);  //4 bytes for count_records, 4 bytes for next_block.
  Record rec;
  BF_Block* block;
  BF_Block_Init(&block);
  
  /*
  !!!!!!This has been replaced by array[indexdesc].buckets!!!!!!!!
  CALL_BF(BF_GetBlock(file_desc,0,block));  
  char* data=BF_Block_GetData(block);
  memcpy(&buckets,data+sizeof(int),sizeof(int));  //Take the number of buckets.
  CALL_BF(BF_UnpinBlock(block));
  */

  if(id!=NULL)
  {
    hash=*id%buckets;  //Hash function.
    num_of_block=(hash*sizeof(int))/BF_BLOCK_SIZE+1;  //Find in which block is the willing bucket.
    CALL_BF(BF_GetBlock(file_desc,num_of_block,block));
    char* data=BF_Block_GetData(block);
    memcpy(&temp,data+hash*sizeof(int),sizeof(int));  //Take the key of the bucket.
    CALL_BF(BF_UnpinBlock(block));

    if(temp==0)
    {
      printf("There isn't any record with record id: %d in this Hash File.\n",*id);  
      return HT_ERROR;
    }

    int count_rec, current_block, flag=0;
    while(flag==0)
    {
      CALL_BF(BF_GetBlock(file_desc,temp,block));
      data=BF_Block_GetData(block);
      memcpy(&count_rec,data,sizeof(int));
      for(int i=0;i<count_rec;i++)
      {
        memcpy(&rec,data+eight_bytes+i*sizeof(Record),sizeof(Record));
        if(rec.id==*id)
        {
          printf("%d,\"%s\",\"%s\",\"%s\"\n\n",rec.id,rec.name,rec.surname,rec.city);
          flag=1;
          break;
        }         
      }
      if(flag==0)
      {
        memcpy(&temp,data+sizeof(int),sizeof(int));
        if(temp==0)
        {
          printf("There isn't any record with record id: %d in this Hash File.\n",*id);
          return HT_ERROR;
        } 
      }
      CALL_BF(BF_UnpinBlock(block)); 
    }
  }
  else
  {
    int num_of_blocks=(buckets*sizeof(int))/BF_BLOCK_SIZE+1, count_rec, no_recs=0;
    
    for(int j=1;j<=num_of_blocks;j++)
    {
      for(int i=0;i<buckets;i++)
      { 
        CALL_BF(BF_GetBlock(file_desc,j,block));
        char* data=BF_Block_GetData(block);
        memcpy(&temp,data+i*sizeof(int),sizeof(int));  //Take the key of the bucket.
        CALL_BF(BF_UnpinBlock(block));       
        
        if(temp==0)  no_recs++;
        else  printf("\n//////////////////\n////BUCKET %d////  \n////////////////\n", i);
 
        while(temp!=0)
        {          
          printf("BLOCK %d \n", temp);
          CALL_BF(BF_GetBlock(file_desc,temp,block));
          char* data1=BF_Block_GetData(block);
          memcpy(&count_rec,data1,sizeof(int));
          for(int k=0;k<count_rec;k++)
          {
            memcpy(&rec,data1+eight_bytes+k*sizeof(Record),sizeof(Record));
            printf("%d,\"%s\",\"%s\",\"%s\"\n",rec.id,rec.name,rec.surname,rec.city);
          }
          memcpy(&temp,data1+sizeof(int),sizeof(int));
          CALL_BF(BF_UnpinBlock(block));          
        }  
      }
    }
    if(no_recs==buckets)
    {
      printf("There isn't any record in this Hash File.\n");
      return HT_ERROR;
    }
  }  
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {

  int num_of_block, hash, temp; 
  int file_desc=array[indexDesc].filedesc;
  int buckets=array[indexDesc].buckets; 
  int eight_bytes=2*sizeof(int);  //4 bytes for count_records, 4 bytes for next_block.

  BF_Block* block;
  BF_Block_Init(&block);
  
  /*
  !!!!!!This has been replaced by array[indexdesc].buckets!!!!!!!!  
  CALL_BF(BF_GetBlock(file_desc,0,block));  
  char* data=BF_Block_GetData(block);
  memcpy(&buckets,data+sizeof(int),sizeof(int));  //Take the number of buckets.
  CALL_BF(BF_UnpinBlock(block));
  */
  
  hash=id%buckets;  //Hash function.
  num_of_block=(hash*sizeof(int))/BF_BLOCK_SIZE+1;  //Find in which block is the willing bucket.
  CALL_BF(BF_GetBlock(file_desc,num_of_block,block));
  char *data=BF_Block_GetData(block);
  memcpy(&temp,data+hash*sizeof(int),sizeof(int));  //Take the key of the bucket.
  CALL_BF(BF_UnpinBlock(block));
  
  //printf("KEY HASH ID %d %d %d \n",temp,hash,id);
  if(temp==0)
  {
    printf("There isn't any record with record id: %d in this Hash File.\n",id);  
    return HT_ERROR;
  }

  int count_rec, current_block, flag=0;
  Record rec;
  while(flag==0)
  {
    //printf("INSIDE WHILE, temp: %d \n",temp);
    CALL_BF(BF_GetBlock(file_desc,temp,block));
    data=BF_Block_GetData(block);
    
    for(int i=0;i<NUM_OF_RECORDS;i++)
    {
      memcpy(&rec,data+eight_bytes+i*sizeof(Record),sizeof(Record));
      
      if(rec.id==id)
      {
        //printf("INSIDE IF1\n");
        current_block=temp;  //Store the number of Block, which has the record that we want to delete.
        memcpy(&temp,data+sizeof(int),sizeof(int));  //Take the number of next block.
        
        //Case: Record to be deleted is in the last block of our list.
        if(temp==0)
        { 
          memcpy(&count_rec,data,sizeof(int));
          //printf("INSIDE IF2, records=%d \n",count_rec);
        
          //Case:Record to be deleted is a random record in our last block, so we need to replace it 
          //with the last record of this block.
          if((i+1)<count_rec)
          {
            memcpy(&rec,data+eight_bytes+(count_rec-1)*sizeof(Record),sizeof(Record));
            memcpy(data+eight_bytes+i*sizeof(Record),&rec,sizeof(Record));
            BF_Block_SetDirty(block);
          }
          
          // i+1 is the n-th Record in our block...
          //Case:Record to be deleted is the last record in our last block, so simply we throw it away.
          //Case: count_rec==(i+1))
          count_rec--;
          memcpy(data,&count_rec,sizeof(int));
          BF_Block_SetDirty(block);
          flag=1;  
          CALL_BF(BF_UnpinBlock(block));
          break;
        }
        //Case: Record to be deleted is in a random block inside our list.
        else
        {  
          //Just unpinned the block, in which there is the Record that is to be deleted.
          CALL_BF(BF_UnpinBlock(block)); 
          //Go to the last block to take the last record.
          while(temp!=0)  
          {
            CALL_BF(BF_GetBlock(file_desc,temp,block));
            data=BF_Block_GetData(block);
            memcpy(&temp,data+sizeof(int),sizeof(int));
            if(temp!=0)
            {  
              CALL_BF(BF_UnpinBlock(block)); 
            }
            else  
            {
              memcpy(&count_rec,data,sizeof(int));
              memcpy(&rec,data+eight_bytes+(count_rec-1)*sizeof(Record),sizeof(Record));  //Taking the last Record of last Block.
              count_rec--; 
              memcpy(data,&count_rec,sizeof(int));
              BF_Block_SetDirty(block);
              CALL_BF(BF_UnpinBlock(block));        
              break;
            }
          }
          CALL_BF(BF_GetBlock(file_desc,current_block,block));
          data=BF_Block_GetData(block);
          memcpy(data+eight_bytes+i*sizeof(Record),&rec,sizeof(Record));
          //memcpy(data+sizeof(int),zero,sizeof(int));///////////////////EDW PROSOXH
          BF_Block_SetDirty(block);
          CALL_BF(BF_UnpinBlock(block));  
          flag=1;
          break;
        }
      }
    }
    
    //Case: Didn't catch our Record...so we have to unpin our block so as to get the next(if it exists).
    if(flag==0)
    {
      memcpy(&temp,data+sizeof(int),sizeof(int));
      CALL_BF(BF_UnpinBlock(block)); 
      if(temp==0)
      {
        printf("There isn't any record to be deleted with record id: %d in this Hash File.\n",id);
        return HT_ERROR;
      } 
    }
  }
  return HT_OK;
}
