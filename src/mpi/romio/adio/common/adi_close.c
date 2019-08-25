/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/* 
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif


/* TODO: need broadcast to other processes
 * to notify them the file is modified and stored in the node */
void ADIOI_GEN_Close(ADIO_File fd, int *error_code)
{
    int err, derr=0;
    static char myname[] = "ADIOI_GEN_CLOSE";

#ifdef ADIOI_MPE_LOGGING
    MPE_Log_event( ADIOI_MPE_close_a, 0, NULL );
#endif
    err = close(fd->fd_sys);
#ifdef ADIOI_MPE_LOGGING
    MPE_Log_event( ADIOI_MPE_close_b, 0, NULL );
#endif
    if (fd->fd_direct >= 0) {
#ifdef ADIOI_MPE_LOGGING
        MPE_Log_event( ADIOI_MPE_close_a, 0, NULL );
#endif
	derr = close(fd->fd_direct);
#ifdef ADIOI_MPE_LOGGING
        MPE_Log_event( ADIOI_MPE_close_b, 0, NULL );
#endif
    }

    fd->fd_sys    = -1;
    fd->fd_direct = -1;

    if (err == -1 || derr == -1) {
	*error_code = ADIOI_Err_create_code(myname, fd->filename, errno);
    }
    else *error_code = MPI_SUCCESS;

    /* TODO: modified below */
    printf("\n------ close...\n");
    PMEMobjpool *pop = (PMEMobjpool *)externpop;
    PMEMblkpool *pbp = (PMEMblkpool *)externpbp;
	TOID(struct stat) pmem_stat = fd->pmem_statp;
	TOID(int) pmem_data = (fd->pmem_datap);
	TOID(int) pmem_lock = (fd->pmem_lockp);
	TOID(int) pmem_update = (fd->pmem_updatep);
    char *filename = fd->filename;

    /* NOTE: optimization: record pmemaddr in open() routine 
     * and save it for further use.
     * then no need to create new pmemfile. 
     * HOWEVER, it seems unable to store pmemaddr, other than reuse it. */

    /* update pmemfile */
    struct pmemfileinfo pfi;
    pfi.data = fd->pmem_datap;
    pfi.lock = fd->pmem_lockp;
    pfi.update = fd->pmem_updatep;
    pfi.stat = fd->pmem_statp;
    pfi.domain_off = fd->domain_off;
    pfi.domain_cnt = fd->domain_cnt;
    pfi.chunks = fd->chunks;
    pfi.offsets = fd->offsets;
    pfi.counts = fd->counts;
    pfi.ranks = fd->ranks;

    /* remove the old pmemfile and create a new one */
    unlink(fd->pmem_filename);
    size_t mapped_len;
    int is_pmem;
    char *pmemaddr;
    if ((pmemaddr = pmem_map_file(fd->pmem_filename, 
                                    sizeof(struct pmemfileinfo),
                                    // tmpoffset + sizeof(TOID(int)), 
                                    PMEM_FILE_EXCL | PMEM_FILE_CREATE, 
                                    0666, &mapped_len, &is_pmem)) == NULL) {
        perror("pmem_map_file in close routine");
    }
    strncpy(pfi.pmemaddr, pmemaddr, 8);
    pmem_memcpy_nodrain(pmemaddr, &pfi, sizeof(struct pmemfileinfo));
    pmem_drain();
    pmem_unmap(pmemaddr, mapped_len);


    /* open original file and write to disk */
    // fprintf(dbfile, "opening oroginal file... ");
    int srcfd, ret = 0;
    struct stat stbuf;
    if ((srcfd = open(filename, O_RDWR | O_EXCL)) < 0) {
        perror("open");
        return ;
    }


    /* writing file back to disk should move to finalize routine */
    /* TODO: consider move to finalized */

    /* calc file size and number of datablks */
    printf("count file size / blocks... ");
    size_t filesize = ((struct stat *)D_RO(pmem_stat))->st_size;
    memcpy(&stbuf, (D_RO(pmem_stat)), sizeof(struct stat));
    int blkcount = (filesize - 1) / PMEMblk_SIZE + 1;
    printf("filesize = %ld, blkcount = %d.\n", filesize, blkcount);

    int bstart = 0;
    int bend = -1;
    off_t pstart = 0;
    off_t pend = 0;

    /* TODO: validate this routine */
    while (bstart < blkcount) {
        
        while (bstart < blkcount && D_RO(pmem_update)[bstart] == 0) {
            bstart++;
        }
        if (bstart >= blkcount) break;
        pstart = bstart * PMEMblk_SIZE;

        bend = bstart;
        while (bend < blkcount && D_RO(pmem_update)[bend] == 1) {
            bend++;
        }
        if (bend >= blkcount) bend = blkcount - 1;
        pend = (bend + 1) * PMEMblk_SIZE - 1;


        size_t buf_len = pend - pstart + 1;
        // if (buf_len > (1 << 25)) buf_len = (1 << 25);
        /* FIXME: if buf too large, this consumes a large amount of memory */
        char *buf = (char *)malloc(buf_len * sizeof(char));
        off_t tmpoff = 0;
        for (int i = bstart; i <= bend; i++) {
            while (D_RO(pmem_lock)[i] == -1) {}
            D_RW(pmem_lock)[i]++;
            pmemblk_read(pbp, buf + tmpoff, D_RO(pmem_data)[i]);
            D_RW(pmem_lock)[i]--;

            tmpoff += PMEMblk_SIZE;
        }

        int dist = fd->domain_off - pstart;
        if (pstart < fd->domain_off) pstart = fd->domain_off;
        if (pend > fd->domain_cnt + fd->domain_off - 1) pend = fd->domain_cnt + fd->domain_off - 1;
        buf_len = pend - pstart + 1;
        printf("offset = %ld, count = %ld\n", fd->domain_off, fd->domain_cnt);

        ret += pwrite(srcfd, buf + pstart, buf_len, pstart);
        // ret += pwrite(srcfd, buf + (pstart - bstart * PMEMblk_SIZE), buf_len, pstart);

        bstart  = bend + 1;
    }


    // for (int i = 0; i < blkcount; i++) {
    //     printf("for blk %d: D_RO(pmem_update)[i] = %d: ", i, D_RO(pmem_update)[i]);
    //     if (D_RO(pmem_update)[i] == 1){
    //         printf("modified... waiting for lock... ");
    //         // check if locked ... TODO: need an elegant lock policy
    //         while (D_RO(pmem_lock)[i] != 0) {
    //         } 
    //         printf("locking... ");
    //         D_RW(pmem_lock)[i] = -1;    // lock 
    //         printf("locked... ");
    //         // when resource is lock, write this blk to 
    //         // corresponding position  in the file
    //         int count = PMEMblk_SIZE;
    //         int offset = PMEMblk_SIZE * i;
    //         if (i == blkcount - 1) {
    //             count = filesize - offset;
    //         }
    //         char tmpbuf[PMEMblk_SIZE];
    //         printf("count = %d, offset = %d... ", count, offset);
    //         pmemblk_read(pbp, tmpbuf, D_RO(pmem_data)[i]);
    //         ret = pwrite(srcfd, tmpbuf, count, offset);
    //         // finish writing
    //         printf("blk written... ");
    //         D_RW(pmem_lock)[i] = 0;     // unlock
    //         D_RW(pmem_update)[i] = 0;
    //         // finish writing
    //     }
    //     printf("done.\n");
    // }

    POBJ_FREE(&pmem_stat);
    POBJ_FREE(&pmem_data);
    POBJ_FREE(&pmem_lock);
    POBJ_FREE(&pmem_update);

    close(srcfd);


    printf("++++++ modified close done.\n");
    /* TODO: modified above */
}
