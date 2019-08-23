/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/* 
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */ 

#include "ad_ufs.h"

void ADIOI_UFS_Open(ADIO_File fd, int *error_code)
{
    int perm, old_mask, amode;
    static char myname[] = "ADIOI_UFS_OPEN";

    if (fd->perm == ADIO_PERM_NULL) {
	old_mask = umask(022);
	umask(old_mask);
	perm = old_mask ^ 0666;
    }
    else perm = fd->perm;

    amode = 0;
    if (fd->access_mode & ADIO_CREATE)
	amode = amode | O_CREAT;
    if (fd->access_mode & ADIO_RDONLY)
	amode = amode | O_RDONLY;
    if (fd->access_mode & ADIO_WRONLY)
	amode = amode | O_WRONLY;
    if (fd->access_mode & ADIO_RDWR)
	amode = amode | O_RDWR;
    if (fd->access_mode & ADIO_EXCL)
	amode = amode | O_EXCL;

    printf("\n------ open\n");
	PMEMobjpool *pop = (PMEMobjpool *)externpop;
    
#ifdef ADIOI_MPE_LOGGING
    MPE_Log_event( ADIOI_MPE_open_a, 0, NULL );
#endif
    fd->fd_sys = open(fd->filename, amode, perm);
#ifdef ADIOI_MPE_LOGGING
    MPE_Log_event( ADIOI_MPE_open_b, 0, NULL );
#endif
    fd->fd_direct = -1;

    if ((fd->fd_sys != -1) && (fd->access_mode & ADIO_APPEND)) {
#ifdef ADIOI_MPE_LOGGING
        MPE_Log_event( ADIOI_MPE_lseek_a, 0, NULL );
#endif
	fd->fp_ind = fd->fp_sys_posn = lseek(fd->fd_sys, 0, SEEK_END);
#ifdef ADIOI_MPE_LOGGING
        MPE_Log_event( ADIOI_MPE_lseek_b, 0, NULL );
#endif
    }

    /* if pmemfile already exist in pmem device */
    printf("fd->pmem_filename: %s\n", fd->pmem_filename);
    printf("if pmemfile already exists...");
    if (access(fd->pmem_filename, F_OK) == 0) {
        printf("Yes.\n");
        int tmpfd = open(fd->pmem_filename, O_RDONLY);
        if (tmpfd < 0) {
            perror("open pmem file");
            exit(1);
        }
        printf("pmemfile opened\n");
        struct pmemfileinfo pfi;
        int ret = pread(tmpfd, &pfi, sizeof(struct pmemfileinfo), 0);
        printf("pread %d bytes\n", ret);
        close(tmpfd);
        printf("pmemfile read to buffer\n");
        /* assign value to variables */
        // printf("%lld, %lld, %lld, %lld\n", pfi.pmemaddr, pfi.stat, pfi.data, pfi.lock);
        fd->pmem_statp = pfi.stat;
        fd->pmem_datap = pfi.data;
        fd->pmem_lockp = pfi.lock;
        fd->pmem_updatep = pfi.update;
        fd->domain_off = pfi.domain_off;
        fd->domain_cnt = pfi.domain_cnt;
        fd->chunks = pfi.chunks;
        fd->offsets = pfi.offsets;
        fd->counts = pfi.counts;
        fd->ranks = pfi.ranks;

        printf("pmemfile parsing finished\n");
        // printf("pmem stat:  %lld \n", fd->pmem_statp);
        printf("++++++ open\n");
        return ;
    } 
    printf("NO. \n");

    /* create a file in pmem path */
    /* FIXME: if fd->filename includes slashes, this fails */
    char *pmemaddr;
    size_t mapped_len;
    int is_pmem;
    if ((pmemaddr = pmem_map_file(fd->pmem_filename, sizeof(struct pmemfileinfo),
            // sizeof(char *) + sizeof(*(fd->filename)) + sizeof(TOID(struct stat)) + 3 * sizeof(TOID(int)), 
            PMEM_FILE_CREATE | PMEM_FILE_EXCL, 0666, &mapped_len, &is_pmem)) == NULL) {
        perror("pmem_map_file");
        exit(1);
    }
    // char *tmpaddr = pmemaddr;
    fd->pmemaddr = pmemaddr;

	struct stat stbuf;

    /* add time info */
    struct timeval tv;
    struct timespec ts;
    gettimeofday(&tv, NULL);
    // TIMEVAL_TO_TIMESPEC(&tv, &ts);
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;

    int ret = fstat(fd->fd_sys, &stbuf);
    memcpy(&(stbuf.st_mtime), &ts, sizeof(ts));
    memcpy(&(stbuf.st_atime), &ts, sizeof(ts));
    memcpy(&(stbuf.st_ctime), &ts, sizeof(ts));

    /* FIXME: how to deala with O_APPEND mode */
	if (ret != 0) {
		/* if file not exists */
		TOID(struct stat) pmem_stat;
		POBJ_ZALLOC(pop, &pmem_stat, struct stat, sizeof(struct stat));

		fd->pmem_statp = pmem_stat;
		fd->pmem_blk_count = 0;

		// create int * obj for data
		TOID(int) pmem_data;
		POBJ_ZALLOC(pop, &pmem_data, int, sizeof(int));
		fd->pmem_datap = pmem_data;
	
		TOID(int) pmem_lock;
		POBJ_ZALLOC(pop, &pmem_lock, int, sizeof(int));
		fd->pmem_lockp = pmem_lock;

		TOID(int) pmem_update;
		POBJ_ZALLOC(pop, &pmem_update, int, sizeof(int));
		fd->pmem_updatep = pmem_update;

	} else {
		/* filename exists: */
		/* creat stat obj for metadata */

		TOID(struct stat) pmem_stat;
		POBJ_ZALLOC(pop, &pmem_stat, struct stat, sizeof(struct stat));
		pmemobj_memcpy_persist(pop, D_RW(pmem_stat), &stbuf, sizeof(struct stat));
		fd->pmem_statp = pmem_stat;

        /* update how many blk the this file covered */
		int filesize = stbuf.st_size;
		int blkcount = (filesize - 1) / PMEMblk_SIZE + 1;
		fd->pmem_blk_count = blkcount;

		/* create int * obj for data */
		TOID(int) pmem_data;
		POBJ_ZALLOC(pop, &pmem_data, int, sizeof(int) * blkcount);
		fd->pmem_datap = pmem_data;

        /* create int * obj for lock */
		TOID(int) pmem_lock;
		POBJ_ZALLOC(pop, &pmem_lock, int, sizeof(int) * blkcount);
		fd->pmem_lockp = pmem_lock;

        /* create int * obj for udpate */
        /* FIXME: wrong then multiple processor update a same blk
         * should use [offset, count] to denote.
         */
		TOID(int) pmem_update;
		POBJ_ZALLOC(pop, &pmem_update, int, sizeof(int) * blkcount);
		fd->pmem_updatep = pmem_update;
	}
    

    /* write infomation to pmem_map_file */
    struct pmemfileinfo pfi;
    strncpy(pfi.pmemaddr, pmemaddr, 8);
    pfi.stat = fd->pmem_statp;
    pfi.lock = fd->pmem_lockp;
    pfi.data = fd->pmem_datap;
    pfi.update = fd->pmem_updatep;
    pfi.domain_off = fd->domain_off;
    pfi.domain_cnt = fd->domain_cnt;
    pfi.chunks = 0;
    pfi.offsets = TOID_NULL(int);
    pfi.counts = TOID_NULL(int);
    pfi.ranks = TOID_NULL(int);


    // printf("%lld, %lld, %lld, %lld\n", pfi.pmemaddr, pfi.stat, pfi.data, pfi.lock);
    pmem_memcpy_nodrain(pmemaddr, &pfi, sizeof(struct pmemfileinfo));
    pmem_drain();
    pmem_unmap(pmemaddr, mapped_len);

    printf("++++++ open .\n");

    if (fd->fd_sys == -1) {
	*error_code = ADIOI_Err_create_code(myname, fd->filename, errno);
    }
    else *error_code = MPI_SUCCESS;
}

// /* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
// /* 
//  *
//  *   Copyright (C) 1997 University of Chicago. 
//  *   See COPYRIGHT notice in top-level directory.
//  */

// #include "ad_ufs.h"

// void ADIOI_UFS_Open(ADIO_File fd, int *error_code)
// {
//     int perm, old_mask, amode;
//     static char myname[] = "ADIOI_UFS_OPEN";

//     if (fd->perm == ADIO_PERM_NULL) {
// 	old_mask = umask(022);
// 	umask(old_mask);
// 	perm = old_mask ^ 0666;
//     }
//     else perm = fd->perm;

//     amode = 0;
//     if (fd->access_mode & ADIO_CREATE)
// 	amode = amode | O_CREAT;
//     if (fd->access_mode & ADIO_RDONLY)
// 	amode = amode | O_RDONLY;
//     if (fd->access_mode & ADIO_WRONLY)
// 	amode = amode | O_WRONLY;
//     if (fd->access_mode & ADIO_RDWR)
// 	amode = amode | O_RDWR;
//     if (fd->access_mode & ADIO_EXCL)
// 	amode = amode | O_EXCL;

    
// #ifdef ADIOI_MPE_LOGGING
//     MPE_Log_event( ADIOI_MPE_open_a, 0, NULL );
// #endif
//     fd->fd_sys = open(fd->filename, amode, perm);
// #ifdef ADIOI_MPE_LOGGING
//     MPE_Log_event( ADIOI_MPE_open_b, 0, NULL );
// #endif
//     fd->fd_direct = -1;

//     if ((fd->fd_sys != -1) && (fd->access_mode & ADIO_APPEND)) {
// #ifdef ADIOI_MPE_LOGGING
//         MPE_Log_event( ADIOI_MPE_lseek_a, 0, NULL );
// #endif
// 	fd->fp_ind = fd->fp_sys_posn = lseek(fd->fd_sys, 0, SEEK_END);
// #ifdef ADIOI_MPE_LOGGING
//         MPE_Log_event( ADIOI_MPE_lseek_b, 0, NULL );
// #endif
//     }


//     if (fd->fd_sys == -1) {
// 	*error_code = ADIOI_Err_create_code(myname, fd->filename, errno);
//     }
//     else *error_code = MPI_SUCCESS;
// }
