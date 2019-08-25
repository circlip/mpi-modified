/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/* 
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */


#include "adio.h" 
#include <unistd.h>

#ifdef AGGREGATION_PROFILE
#include "mpe.h"
#endif
#ifdef ROMIO_GPFS
# include "adio/ad_gpfs/ad_gpfs_tuning.h"
#endif

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif


/* TODO: modified below */
/* replace all pread()s with this function */ 
int pread_and_copy2pmem(ADIO_File fd, int srcfd, void *buf, size_t count, off_t offset) {
	/* prepare variables */
	printf("\n------ read\n");
	printf("read from file of size = %ld\n", D_RO(fd->pmem_statp)->st_size);
	int ans = 0;
	PMEMblkpool *pbp = (PMEMblkpool *)externpbp;
	PMEMobjpool *pop = (PMEMobjpool *)externpop;
	TOID(struct stat) pmem_stat = fd->pmem_statp;
	TOID(int) pmem_data = (fd->pmem_datap);
	TOID(int) pmem_lock = (fd->pmem_lockp);
	TOID(int) pmem_update = (fd->pmem_updatep);

	/* calc blks */
	int startblk = offset / PMEMblk_SIZE;
	off_t startbit = startblk * PMEMblk_SIZE;
	int endblk = (offset + count - 1) / PMEMblk_SIZE;
	off_t endbit = (endblk + 1) * PMEMblk_SIZE - 1;
	struct stat stbuf;
	if (fstat(srcfd, &stbuf) != 0) {
		perror("fstat");
		return errno;
	}

	size_t filesize = D_RO(pmem_stat)->st_size;
	if (endbit > filesize - 1) {
		endbit = filesize - 1;
	}
	if (endbit > offset + count - 1) {
		endbit = offset + count - 1;
	}
	// if (endbit > stbuf.st_size - 1) {
	// 	endbit = stbuf.st_size - 1;
	// }
	size_t nbits = endbit + 1 - startbit;
    printf("startblk: %d,  startbit: %ld,   endblk: %d,  endbit: %ld   nbits: %ld\n", 
            startblk, startbit, endblk, endbit, nbits);

	off_t pstart = startbit;
	int pend = pstart - 1;
	int bstart = startblk;
	int bend = startblk;
	// int tmpblkcount = 0;
	off_t buf_offset = 0;

	/* TODO: should verified this routine */
	/* iterate each data blk.
	 * if empty, pread from original file and written to pmemblk
	 * otherwise, pmemblk_read from pmemblk */
	while (bstart <= endblk) {
		/* calc [pstart, pend] that need to pread from disk */
		printf("pstart = %ld, pend = %d\n", pstart, pend);
		int i = bstart;
		printf("pmemdata[i] = %d\n", D_RO(pmem_data)[i]);
		while (i <= endblk && D_RO(pmem_data)[i] == 0) {
			printf("hit--- pend = %d\n", pend);
			pend += PMEMblk_SIZE;
			bend = i;
			// tmpblkcount ++;
			i++;
		}
		printf("pstart = %ld, pend = %d\n", pstart, pend);
		if (pend > (int)(offset + count - 1)) {
			printf("offset + count = %ld\n", offset + count);
			pend = offset + count - 1;
		} 
		/* FIXME: with pstart modified, this only reads requested data.
		 * should it or not read from the start of a blk */
		if (pstart < offset) {
			pstart = offset;
		}

		printf("pstart = %ld, pend = %d\n", pstart, pend);
		if (pend > pstart) {
			/* pread [pstart, pend] from original file */
			size_t bufsize = pend - pstart + 1;
			char *tmpbuf = (char *)calloc(bufsize, sizeof(char));
			int res = pread(srcfd, tmpbuf, bufsize, pstart);
			ans += res;


			/* copy to buf */
			memcpy(buf + buf_offset, tmpbuf, bufsize);
			buf_offset += bufsize;

			/* write from tmpbuf to pmemblk for cache */
			for (int j = bstart; j <= bend; j++) {
				while (D_RO(pmem_lock)[j] != 0) {}
				D_RW(pmem_lock)[j] = -1;
				D_RW(pmem_data)[j] = pmemblk_request(); 
				// D_RW(pmem_data)[j] = __sync_fetch_and_add(&PMEMblk_curcount, 1L);
				/* TODO: what happens if j is the last blk of the file
				 * that is, if the data to be wriiten to pmemblk is smaller than a blksize 
				 * what will the pmemblk contain? */
				pmemblk_write(pbp, tmpbuf + (j - bstart) * PMEMblk_SIZE, D_RO(pmem_data)[j]);
				D_RW(pmem_lock)[j] = 0;
			}
			free(tmpbuf);
		}

		printf("prepare reading from pmem cached buffer: blk # %d\n", D_RO(pmem_data)[i]);
		char *tmpbuf = (char *)calloc(PMEMblk_SIZE, sizeof(char));
		/* read cached blk to buf */
		while (i <= endblk && D_RO(pmem_data)[i] > 0) {
			size_t bufsize;
			while (D_RO(pmem_lock)[i] == -1) {} /* being written, shall not read now */
			D_RW(pmem_lock)[i] ++;
			pmemblk_read(pbp, tmpbuf, D_RO(pmem_data)[i]);
			D_RW(pmem_lock)[i] --;
			printf("a pmemblk read from pmemblk to dram\n");

			/* calc bufsize of useful info */
			if (i == endblk) {
				bufsize = endbit - PMEMblk_SIZE * i + 1;
			} else {
				bufsize = PMEMblk_SIZE;
			}
			if (i == startblk) {
				bufsize -= (pstart - startbit);
				memcpy(buf + buf_offset, tmpbuf + (pstart - startbit), bufsize);
			} else {
				memcpy(buf + buf_offset, tmpbuf, bufsize);
			}
			printf("bufsize == %ld\n", bufsize);
			ans += bufsize;
			buf_offset += bufsize;
			i++;
		}

		/* update pstart, bstart */
		bstart = i;
		bend = bstart - 1;
		pstart = i * PMEMblk_SIZE;
		pend = pstart - 1;
		free(tmpbuf);
	}
	printf("main routine of read completed\n");
	/* modify access time */
	int ret = fstat(srcfd, &stbuf);
	struct timespec ts;
	long *sec_list;
	unsigned long *nsec_list;
	int *offsets;
	int *counts;
	int *ranks;
	int chunks;

	memcpy(&ts, &(stbuf.st_atim), sizeof(struct timespec));

	/* gather timespec and find the lastest one, and then broadcast it */
	int myrank;
	int nprocs;
	MPI_Comm_size(fd->comm, &nprocs);
	MPI_Comm_rank(fd->comm, &myrank);

	if (myrank == fd->hints->ranklist[0]) {
		sec_list = (long *)calloc(nprocs, sizeof(long));
		nsec_list = (unsigned long *)calloc(nprocs, sizeof(unsigned long));
		offsets = (int *)calloc(nprocs, sizeof(int));
		counts = (int *)calloc(nprocs, sizeof(int));
		ranks = (int *)calloc(nprocs, sizeof(int));
		for (int i = 0; i < nprocs; i++) {
			ranks[i] = i;
		}
	}
	MPI_Gather(&(ts.tv_sec), 1, MPI_LONG, sec_list, 1, MPI_LONG, fd->hints->ranklist[0], fd->comm);
	MPI_Gather(&offset, 1, MPI_INT, offsets, 1, MPI_INT, fd->hints->ranklist[0], fd->comm);
	MPI_Gather(&count, 1, MPI_INT, counts, 1, MPI_INT, fd->hints->ranklist[0], fd->comm);

	int maxpos = 0;
	if (myrank == fd->hints->ranklist[0]) {
		for (int i = 1; i < nprocs; i++) {
			if (sec_list[i] > sec_list[maxpos]) maxpos = i;
		}	
		ts.tv_sec = sec_list[maxpos];
		ts.tv_nsec = nsec_list[maxpos];

		/* merge offset and count list */
		if (fd->chunks == 0) {
			/* qsort within offsetlist and count list */
			qsort_offset_and_count(offsets, counts, ranks, 0, nprocs);
		} else {
			/* load to memory and insert new records */
			int *tmpoffsets = (int *)calloc(nprocs + fd->chunks, sizeof(int));
			int *tmpcounts = (int *)calloc(nprocs + fd->chunks, sizeof(int));
			int *tmpranks = (int *)calloc(nprocs + fd->chunks, sizeof(int));
			int tmpchunks = 0;
			// for (int i = 0; i < fd->chunks; i++) {
			// 	tmpoffsets[i] = D_RO(fd->offsets)[i];
			// 	tmpcounts[i] = D_RO(fd->counts)[i];
			// 	tmpranks[i] = D_RO(fd->ranks)[i];
			// }
			qsort_offset_and_count(offsets, counts, ranks, 0, nprocs);
			
			int i = 0, j = 0;
			while (i < fd->chunks && j < nprocs) {
				/* merge sort */
				if (D_RO(fd->offsets)[i] < offsets[j]) {
					int iend = D_RO(fd->offsets)[i] + D_RO(fd->counts)[i];
					int jend = offsets[j] + counts[j];
					if (iend < offsets[j]) {
						tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
						tmpcounts[tmpchunks] = D_RO(fd->counts)[i];
						tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
						tmpchunks++;
						i++;
					} else if (iend == offsets[j]) {
						if (D_RO(fd->ranks)[i] == ranks[j]) {
							D_RW(fd->counts)[i] += counts[j];
							j++;
							/* should post-process to merge neighboring [] */
						} else {
							tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
							tmpcounts[tmpchunks] = D_RO(fd->counts)[i];
							tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
							tmpchunks++;
							i++;
						}
					} else if (iend < jend) {
						tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
						tmpcounts[tmpchunks] = D_RO(fd->counts)[i] - (iend - offsets[j]);
						tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
						tmpchunks++;
						i++;
					} else {
						/* i starts earlier and ends later */
						tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
						tmpcounts[tmpchunks] = D_RO(fd->counts)[i];
						tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
						tmpchunks++;
						i++;
						j++;
					}
				} else if (D_RO(fd->offsets)[i] == offsets[j]) {
					/* simply choose the larger one */
					if (D_RO(fd->counts)[i] > counts[j]) {
						tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
						tmpcounts[tmpchunks] = D_RO(fd->counts)[i];
						tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
						tmpchunks++;
						i++;
					} else {
						/* choose the newer one */
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						j++;
						i++;
					}
				} else { /* j starts earlier */
					int jend = offsets[j] + counts[j];
					int iend = D_RO(fd->offsets)[i] + D_RO(fd->counts)[i];
					if (jend < D_RO(fd->offsets)[i]) {
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						j++;
					} else if (jend == D_RO(fd->offsets)[i]) {
						if (ranks[j] == D_RO(fd->ranks)[i]) {
							counts[j] += D_RO(fd->counts)[i];
							i++;
						} else {
							tmpoffsets[tmpchunks] = offsets[j];
							tmpcounts[tmpchunks] = counts[j];
							tmpranks[tmpchunks] = ranks[j];
							tmpchunks++;
							j++;
						}
					} else if (jend < iend) { /* j ends later than i starts */
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j] - (jend - D_RO(fd->offsets)[i]);
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						j++;
					} else { /* j starts earlier and ends later */
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						i++;
						j++;
					}
				}
			}
			while (i < fd->chunks) {
				tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
				tmpcounts[tmpchunks] = D_RO(fd->counts)[i];
				tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
				tmpchunks++;
				i++;
			}
			while (j < nprocs) {
				tmpoffsets[tmpchunks] = offsets[j];
				tmpcounts[tmpchunks] = counts[j];
				tmpranks[tmpchunks] = ranks[j];
				tmpchunks++;
				j++;
			}

			/* post-process */
			int f = 1;
			int s = 0;
			int t = tmpchunks;
			while (f < tmpchunks) {
				if (tmpranks[s] == tmpranks[f] && tmpoffsets[s] + tmpcounts[s] == tmpoffsets[f]) {
					tmpoffsets[s] = tmpoffsets[s];
					tmpcounts[s] = tmpcounts[s] + tmpcounts[f];
					tmpranks[s] = tmpranks[s];
					f++;
					t--;
				} else {
					s++;
					tmpoffsets[s] = tmpoffsets[f];
					tmpcounts[s] = tmpcounts[f];
					tmpranks[s] = tmpranks[f];
					f++;
				}
			}

			tmpoffsets = (int *)realloc(tmpoffsets, t * sizeof(int));
			tmpcounts = (int *)realloc(tmpcounts, t * sizeof(int));
			tmpranks = (int *)realloc(tmpranks, t * sizeof(int));
			tmpchunks = t;


			chunks = tmpchunks;
			free(offsets);
			free(counts);
			free(ranks);
			offsets = tmpoffsets;
			counts = tmpcounts;
			ranks = tmpranks;
		}

	}

	MPI_Bcast(&(ts.tv_sec), 1, MPI_LONG, fd->hints->ranklist[0], fd->comm);
	MPI_Bcast(&(ts.tv_nsec), 1, MPI_UNSIGNED_LONG, fd->hints->ranklist[0], fd->comm);

	MPI_Bcast(&chunks, 1, MPI_INT, fd->hints->ranklist[0], fd->comm);
	if (myrank != fd->hints->ranklist[0]) {
		offsets = (int *)calloc(chunks, sizeof(int));
		counts = (int *)calloc(chunks, sizeof(int));
		ranks = (int *)calloc(chunks, sizeof(int));
	}

	MPI_Bcast(offsets, chunks, MPI_INT, fd->hints->ranklist[0], fd->comm);
	MPI_Bcast(counts, chunks, MPI_INT, fd->hints->ranklist[0], fd->comm);
	MPI_Bcast(ranks, chunks, MPI_INT, fd->hints->ranklist[0], fd->comm);

	/* update pmemobj */
	POBJ_REALLOC(pop, &(fd->offsets), int, chunks * sizeof(int));
	pmemobj_memcpy_persist(pop, D_RW(fd->offsets), offsets, chunks * sizeof(int));
	POBJ_REALLOC(pop, &(fd->counts), int, chunks * sizeof(int));
	pmemobj_memcpy_persist(pop, D_RW(fd->counts), counts, chunks * sizeof(int));
	POBJ_REALLOC(pop, &(fd->ranks), int, chunks * sizeof(int));
	pmemobj_memcpy_persist(pop, D_RW(fd->ranks), ranks, chunks * sizeof(int));


	D_RW(pmem_stat)->st_atim.tv_sec = ts.tv_sec;
	D_RW(pmem_stat)->st_atim.tv_nsec = ts.tv_nsec;

	/* update pmem file: this leaves for adi_close.c */
	


    printf("++++++ read: count = %d, done.\n", ans);
	return ans;


	// /* read to tmpbuf */
    // /* TODO: if already loaded to pmem, no need to peform pread() */
	// char *tmpbuf = (char *)malloc(nbits * sizeof(char));
	// int res = pread(srcfd, tmpbuf, nbits, startbit);	
	// if (res != nbits) {
	// 	perror("pread");
	// 	return res;
	// }

	// char tmptmpbuf[PMEMblk_SIZE];
	// off_t tmpoffset = 0;

    // int i;
	// for (i = startblk; i < endblk; i++) {
	// 	//  TODO: need more elegant lock policy
	// 	memcpy(tmptmpbuf, tmpbuf + tmpoffset, PMEMblk_SIZE);
    //     printf("pmemblk: %d, pmemblcurcount: %lld, tmpoffset: %ld\n", i, PMEMblk_curcount, tmpoffset);
    //     while (D_RO(pmem_lock)[i] == -1) {}
    //     D_RW(pmem_lock)[i]++;
    //     D_RW(pmem_data)[i] = __sync_fetch_and_add(&PMEMblk_curcount, 1L);
    //     pmemblk_write(pbp, tmptmpbuf, D_RO(pmem_data)[i]);
    //     D_RW(pmem_lock)[i]--;
    //     tmpoffset += PMEMblk_SIZE;
	// }
	// // the last blk: 
	// // NOTICE: even the sum of the blks size may be larger than the file,
	// // the file size is not modified so feel free to write a whole blk down to pbp
    // if (tmpoffset < count + offset) {
	//     memcpy(tmptmpbuf, tmpbuf + tmpoffset, endbit - tmpoffset + 1);
    //     while (D_RO(pmem_lock)[i] == -1) {}
    //     D_RW(pmem_lock)[i]++;
    //     D_RW(pmem_data)[i] = __sync_fetch_and_add(&PMEMblk_curcount, 1L);
    //     pmemblk_write(pbp, tmptmpbuf, D_RO(pmem_data)[i]);
    //     D_RW(pmem_lock)[i]--;
    //     tmpoffset += PMEMblk_SIZE;
    // }
	
	// memcpy(buf, tmpbuf + (offset - startbit), count);
	// free(tmpbuf);
    // printf("++++++ read: count = %ld, done.\n", count);
	// return count;
}
/* TODO: modified above */


void ADIOI_GEN_ReadContig(ADIO_File fd, void *buf, int count, 
			  MPI_Datatype datatype, int file_ptr_type,
			  ADIO_Offset offset, ADIO_Status *status,
			  int *error_code)
{
    printf("ADIOI_GEN_ReadContig...\n");
    ssize_t err = -1;
    MPI_Count datatype_size;
    ADIO_Offset len, bytes_xfered=0;
    size_t rd_count;
    static char myname[] = "ADIOI_GEN_READCONTIG";
#ifdef ROMIO_GPFS
    double io_time=0;
#endif
    char *p;

#ifdef AGGREGATION_PROFILE
    MPE_Log_event (5034, 0, NULL);
#endif
    MPI_Type_size_x(datatype, &datatype_size);
    len = datatype_size * (ADIO_Offset)count;

#ifdef ROMIO_GPFS
    io_time = MPI_Wtime();
    if (gpfsmpio_timing) {
	gpfsmpio_prof_cr[ GPFSMPIO_CIO_DATA_SIZE ] += len;
    }
#endif

    if (file_ptr_type == ADIO_INDIVIDUAL) {
	offset = fd->fp_ind;
    }

    p=buf;
    while (bytes_xfered < len) {
#ifdef ADIOI_MPE_LOGGING
	MPE_Log_event( ADIOI_MPE_read_a, 0, NULL );
#endif
	rd_count = len - bytes_xfered;
	/* stupid FreeBSD and Darwin do not like a count larger than a signed
           int, even though size_t is eight bytes... */
        if (rd_count > INT_MAX)
            rd_count = INT_MAX;
#ifdef ROMIO_GPFS
	if (gpfsmpio_devnullio)
	    err = pread(fd->null_fd, p, rd_count, offset+bytes_xfered);
	else
#endif
        err = pread_and_copy2pmem(fd, fd->fd_sys, p, rd_count, offset+bytes_xfered);
	    // err = pread(fd->fd_sys, p, rd_count, offset+bytes_xfered);
	/* --BEGIN ERROR HANDLING-- */
	if (err == -1) {
	    *error_code = MPIO_Err_create_code(MPI_SUCCESS,
		    MPIR_ERR_RECOVERABLE,
		    myname, __LINE__,
		    MPI_ERR_IO, "**io",
		    "**io %s", strerror(errno));
	    fd->fp_sys_posn = -1;
	    return;
	}
	/* --END ERROR HANDLING-- */
	if (err == 0) {
	    /* end of file */
	    break;
	}

#ifdef ADIOI_MPE_LOGGING
	MPE_Log_event( ADIOI_MPE_read_b, 0, NULL );
#endif
	bytes_xfered += err;
	p += err;
    }
#ifdef ROMIO_GPFS
    if (gpfsmpio_timing) gpfsmpio_prof_cr[ GPFSMPIO_CIO_T_POSI_RW ] += (MPI_Wtime() - io_time);
#endif
    fd->fp_sys_posn = offset + bytes_xfered;

    if (file_ptr_type == ADIO_INDIVIDUAL) {
	fd->fp_ind += bytes_xfered; 
    }

#ifdef HAVE_STATUS_SET_BYTES
    /* what if we only read half a datatype? */
    /* bytes_xfered could be larger than int */
    if (err != -1) MPIR_Status_set_bytes(status, datatype, bytes_xfered);
#endif

    *error_code = MPI_SUCCESS;
#ifdef AGGREGATION_PROFILE
    MPE_Log_event (5035, 0, NULL);
#endif
#ifdef ROMIO_GPFS
    if (gpfsmpio_timing) gpfsmpio_prof_cr[ GPFSMPIO_CIO_T_MPIO_RW ] += (MPI_Wtime() - io_time);
#endif
}
