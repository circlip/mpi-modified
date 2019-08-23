/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/* 
 *
 *   Copyright (C) 2004 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */


#include "adio.h"
#include <unistd.h>

#ifdef AGGREGATION_PROFILE
#include "mpe.h"
#endif 

#ifdef ROMIO_GPFS
#include "adio/ad_gpfs/ad_gpfs_tuning.h"
#endif

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif
/* TODO: need a update list to prevent multiple processes updating same bytes */
/* TODO: need timestamps in the update list to invalidate old updates */
/* TODO: write at different location: apply for new pmemblk and invalidate the old one
 * atomicity needed */
/* TODO: modified below */
/* replace all pwrite() with this function */
int pwrite2pmem(ADIO_File fd, int srcfd, void *buf, size_t count, off_t offset) {
	/* prepare variables */
    printf("\n------ write: filesize = %ld\n", D_RO(fd->pmem_statp)->st_size);
    // printf("\n------ write: filesize = %ld\n", D_RO(fd->pmem_statp)->st_size);
	PMEMblkpool *pbp = (PMEMblkpool *)externpbp;
	PMEMobjpool *pop = (PMEMobjpool *)externpop;
	TOID(struct stat) pmem_stat = fd->pmem_statp;
	TOID(int) pmem_data = fd->pmem_datap;
	TOID(int) pmem_lock = fd->pmem_lockp;
	TOID(int) pmem_update = fd->pmem_updatep;
	/* TODO: this should flush to pmem right away */
	fd->domain_off = offset;
	fd->domain_cnt = count;
	// if (pmem_stat == NULL) {
	// 	printf("MPI_write: pmem_stat is a null pointer\n");
	// } else {
	// 	printf("MPI_write: pmem_stat not null\n");
	// }

	/* modify file size */
    int blkcount = 0;
	size_t filesize = D_RO(pmem_stat)->st_size;
	if (offset + count > filesize) {
		filesize = offset + count;
		D_RW(pmem_stat)->st_size = offset + count;
		blkcount = (offset + count - 1) / PMEMblk_SIZE + 1;
		POBJ_ZREALLOC(pop, &pmem_data, int, blkcount * sizeof(int));
		POBJ_ZREALLOC(pop, &pmem_lock, int, blkcount * sizeof(int));
		POBJ_ZREALLOC(pop, &pmem_update, int, blkcount * sizeof(int));
		/* TODO: should verify if the former values change when zrealloc-ed */
        fd->pmem_datap = pmem_data;
        fd->pmem_lockp = pmem_lock;
        fd->pmem_updatep = pmem_update;
	} else {
        blkcount = (filesize - 1) / PMEMblk_SIZE + 1;
    }
    printf("recalc file size: size = %ld, blkcount = %d\n", filesize, blkcount);

	// calc blks
	int startblk = offset / PMEMblk_SIZE;
	int startbit = startblk * PMEMblk_SIZE;
	int endblk = (offset - 1 + count) / PMEMblk_SIZE;
	int endbit = endblk * PMEMblk_SIZE - 1 + PMEMblk_SIZE;
	if (endbit > D_RO(pmem_stat)->st_size - 1) {
		endbit = D_RO(pmem_stat)->st_size - 1;
	} 
	int nbits = endbit - startbit + 1;
    printf("calc blk: startblk = %d, startbit = %d, endblk = %d, endbit = %d, nbits = %d\n",
            startblk, startbit, endblk, endbit, nbits);
	int curblk = startblk;
	int curoff = 0;
	int written = 0;
	long long pmemblk_no;
	/* if doesn't start at startbit, should read startblk and than write to it */
	if (offset > startbit) {
		/* read startblk */
        printf("first blk: offset > startbit\n");
		if (D_RO(pmem_data)[startblk] == 0) {
			printf("should read from file\n");
			/* should read from file */
			char tmpbuf[PMEMblk_SIZE];
			int res = pread(srcfd, tmpbuf, PMEMblk_SIZE, startbit);
			if (res < PMEMblk_SIZE) {
				printf("111111\n");
				/* file ends before the blk ends */
				memcpy(tmpbuf + (offset - startbit), buf, count);
			} else {
				printf("222222\n");
				memcpy(tmpbuf + (offset - startbit), buf, PMEMblk_SIZE - (offset - startbit) + 1);
			}
			while (D_RO(pmem_lock)[startblk] != 0) {}
			D_RW(pmem_lock)[startblk] = -1;
			D_RW(pmem_data)[startblk] = pmemblk_request();
			// D_RW(pmem_data)[startblk] = __sync_fetch_and_add(&PMEMblk_curcount, 1);
			pmemblk_write(pbp, tmpbuf, D_RO(pmem_data)[startblk]);
            D_RW(pmem_update)[startblk] = 1;
			D_RW(pmem_lock)[startblk] = 0;
			written += res;
			curoff += res;
		} else {
			printf("no need to read from file\n");
			char tmpbuf[PMEMblk_SIZE];
			while (D_RO(pmem_lock)[startblk] == -1) {}
			D_RW(pmem_lock)[startblk] ++;
			pmemblk_read(pbp, tmpbuf, D_RO(pmem_data)[startblk]);
			D_RW(pmem_lock)[startblk] --;
			if (offset + count < startbit - 1 + PMEMblk_SIZE) {
				printf("ends before blk ends\n");
				memcpy(tmpbuf + (offset - startbit), buf, count);
				printf("%s\n", tmpbuf);
				written += count;
				curoff += count;
			} else {
				printf("444444\n");
				memcpy(tmpbuf + (offset - startbit), buf, PMEMblk_SIZE + 1 - (offset - startbit));
				written += PMEMblk_SIZE + 1 - (offset - startbit);
				curoff += PMEMblk_SIZE + 1 - (offset - startbit); 
			}
			while (D_RO(pmem_lock)[startblk] != 0) {}
			D_RW(pmem_lock)[startblk] = -1;
			int blkno = pmemblk_request();
			pmemblk_write(pbp, tmpbuf, blkno);
			pmemblk_release(D_RO(pmem_data)[startblk]);
			D_RW(pmem_data)[startblk] = blkno;
            D_RW(pmem_update)[startblk] = 1;
			D_RW(pmem_lock)[startblk] = 0;
		}
		curblk ++;
	}
	/* for full blks */
    printf("for full blks\n");
	for (int i = curblk; i < endblk; i++) {
        printf("i = %d, curblk = %d, endblk = %d\n", i, curblk, endblk);
		if (D_RO(pmem_data)[i] == 0) {
			D_RW(pmem_data)[i] = pmemblk_request();
			// D_RW(pmem_data)[i] = __sync_fetch_and_add(&PMEMblk_curcount, 1);
		} else {
			pmemblk_release(D_RO(pmem_data)[i]);
			D_RW(pmem_data)[i] = pmemblk_request();
		}
		while (D_RO(pmem_lock)[i] != 0) {}
		D_RW(pmem_lock)[i] = -1;
		pmemblk_write(pbp, buf + curoff, D_RO(pmem_data)[i]);
        D_RW(pmem_update)[i] = 1;
		D_RW(pmem_lock)[i] = 0;
		written += PMEMblk_SIZE;
		curoff += PMEMblk_SIZE;
		curblk ++;
	}
	/* last blk */ 
	if (curblk == endblk) {
        printf("last blk: \n");
		char tmpbuf[PMEMblk_SIZE];
		memcpy(tmpbuf, buf + curoff, count - written);
		if (D_RO(pmem_data)[endblk] == 0) {
			D_RW(pmem_data)[endblk] = pmemblk_request();
			// D_RW(pmem_data)[endblk] = __sync_fetch_and_add(&PMEMblk_curcount, 1L);
		} else {
			pmemblk_release(D_RO(pmem_data)[endblk]);
			D_RW(pmem_data)[endblk] = pmemblk_request();
		}
        printf("waiting for lock... %d", D_RO(pmem_lock)[endblk]);
		while (D_RO(pmem_lock)[endblk] != 0) {}
        printf("done\n");

		D_RW(pmem_lock)[endblk] = -1;
		pmemblk_write(pbp, tmpbuf, D_RO(pmem_data)[endblk]);
        D_RW(pmem_update)[endblk] = 1;
		D_RW(pmem_lock)[endblk] = 0;
	}
	printf("write file of size = %ld\n", filesize);
	printf("written. metadata next: \n");
    // for (int i = startblk; i <= endblk; i++) {
    //     printf("%d:%d  ", i, D_RO(pmem_update)[i]);
    // }
    // printf("\n");
    // pmemobj_persist(pop, D_RW(pmem_update), sizeof(int));

	/* modify fd->filesize and modify time */
	struct timespec ts;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	ts.tv_sec = tv.tv_sec;
	ts.tv_nsec = tv.tv_usec * 1000;

	long *sec_list = NULL;
	unsigned long *nsec_list = NULL;

	int maxpos = 0;
	filesize = D_RO(fd->pmem_statp)->st_size;
	size_t tmpfilesize = filesize;
	size_t *filesize_list = NULL;
	printf("filesize set\n");

	/* container for gathering info from each process */
	int *offsets;
	int *counts;
	int *ranks;
	int chunks;

	int myrank;
	int nprocs;
	MPI_Comm_size(fd->comm, &nprocs);
	MPI_Comm_rank(fd->comm, &myrank);
	printf("ranklist[0] = %d\n", fd->hints->ranklist[0]);

	if (myrank ==  fd->hints->ranklist[0]) {
		/* gather filesize from all other processors */
		filesize_list = (size_t *)calloc(nprocs, sizeof(size_t));
		sec_list = (long *)calloc(nprocs, sizeof(long));
		nsec_list = (unsigned long *)calloc(nprocs, sizeof(unsigned long));
		offsets = (int *)calloc(nprocs, sizeof(int));
		counts = (int *)calloc(nprocs, sizeof(int));
		ranks = (int *)calloc(nprocs, sizeof(int));
		for (int  i = 0; i < nprocs; i++) {
			ranks[i] = i;
		}
	} 
	int ret = MPI_Gather(&tmpfilesize, 1, MPI_UNSIGNED_LONG, filesize_list, 1, MPI_UNSIGNED_LONG, fd->hints->ranklist[0], fd->comm);
		printf("=========================================================== ret = %d\n", ret);
	MPI_Gather(&(ts.tv_sec), 1, MPI_LONG, sec_list, 1, MPI_LONG, fd->hints->ranklist[0], fd->comm);
	MPI_Gather(&(ts.tv_nsec), 1, MPI_UNSIGNED_LONG, nsec_list, 1, MPI_UNSIGNED_LONG, fd->hints->ranklist[0], fd->comm);
	printf("gathered\n");

	if (myrank ==  fd->hints->ranklist[0]) {
		// printf("111111\n");
		printf("#######: ");
		for (int i = 0; i < nprocs; i++) {
			printf(" %d: %ld   ", i, filesize_list[i]);
			if (filesize_list[i] > tmpfilesize) {
				tmpfilesize = filesize_list[i];
			}
		}
		printf("\n");
		free(filesize_list);
		// printf("22222\n");
		for (int i = 1; i < nprocs; i++) {
			if (sec_list[i] > sec_list[maxpos]) maxpos = i;
		}
		ts.tv_sec = sec_list[maxpos];
		ts.tv_nsec = nsec_list[maxpos];
		free(sec_list);
		free(nsec_list);


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
				/* merge sort: j taks priority */
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
					} else if (iend <= jend) {
						tmpoffsets[tmpchunks] = D_RO(fd->offsets)[i];
						tmpcounts[tmpchunks] = D_RO(fd->counts)[i] - (iend - offsets[j]);
						tmpranks[tmpchunks] = D_RO(fd->ranks)[i];
						tmpchunks++;
						i++;
					} else { /* i starts earlier and ends later */
						/* FIXME: this chop a [] into 2, may incur "out-of-range" error */
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						j++;
					}
				} else if (D_RO(fd->offsets)[i] == offsets[j]) {
					/* simply choose the newer one */
					if (D_RO(fd->counts)[i] > counts[j]) {
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;

						D_RW(fd->offsets)[i] += counts[j];
						D_RW(fd->counts)[i] -= counts[j];
						j++;
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
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						D_RW(fd->offsets)[i] = offsets[j] + counts[j];
						D_RW(fd->counts)[i] = jend - iend;
						j++;
					} else { /* j starts earlier end ends later */
						tmpoffsets[tmpchunks] = offsets[j];
						tmpcounts[tmpchunks] = counts[j];
						tmpranks[tmpchunks] = ranks[j];
						tmpchunks++;
						j++;
						i++;
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
	// printf("33333\n") 
	MPI_Bcast(&tmpfilesize, 1, MPI_UNSIGNED_LONG, fd->hints->ranklist[0], fd->comm);
	printf("after bcast, tmpfilesize= %ld\n", tmpfilesize);
	if (tmpfilesize > filesize) {
		int tmpblkcount = (tmpfilesize - 1) / PMEMblk_SIZE + 1;
		POBJ_ZREALLOC(pop, &pmem_data, int, tmpblkcount * sizeof(int));
		POBJ_ZREALLOC(pop, &pmem_lock, int, tmpblkcount * sizeof(int));
		POBJ_ZREALLOC(pop, &pmem_update, int, tmpblkcount * sizeof(int));
		fd->pmem_datap = pmem_data;
		fd->pmem_lockp = pmem_lock;
		fd->pmem_updatep = pmem_update;
	}
	filesize = tmpfilesize;
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

	printf("broadcasted\n");

	fd->filesize = filesize;
	/* TODO: update pmem filesize in other nodes */
	D_RW(pmem_stat)->st_size = filesize;
	D_RW(pmem_stat)->st_mtim.tv_sec = ts.tv_sec;
	D_RW(pmem_stat)->st_mtim.tv_nsec = ts.tv_nsec;
	printf("pmem stat updated\n");


    printf("++++++ write\n");
	return count;
}
/* TODO: modified above */

void ADIOI_GEN_WriteContig(ADIO_File fd, const void *buf, int count,
			   MPI_Datatype datatype, int file_ptr_type,
			   ADIO_Offset offset, ADIO_Status *status,
			   int *error_code)
{
    ssize_t err = -1;
    MPI_Count datatype_size;
    ADIO_Offset len, bytes_xfered=0;
    size_t wr_count;
    static char myname[] = "ADIOI_GEN_WRITECONTIG";
#ifdef ROMIO_GPFS
    double io_time=0;
#endif
    char * p;

#ifdef AGGREGATION_PROFILE
    MPE_Log_event (5036, 0, NULL);
#endif

    MPI_Type_size_x(datatype, &datatype_size);
    len = (ADIO_Offset)datatype_size * (ADIO_Offset)count;

#ifdef ROMIO_GPFS
    io_time = MPI_Wtime();
    if (gpfsmpio_timing) {
	gpfsmpio_prof_cw[ GPFSMPIO_CIO_DATA_SIZE ] += len;
    }
#endif

    if (file_ptr_type == ADIO_INDIVIDUAL) {
	offset = fd->fp_ind;
    }

    p = (char *)buf;
    while (bytes_xfered < len) {
#ifdef ADIOI_MPE_LOGGING
	MPE_Log_event( ADIOI_MPE_write_a, 0, NULL );
#endif
	wr_count = len - bytes_xfered;
	/* Frustrating! FreeBSD and OS X do not like a count larger than 2^31 */
        if (wr_count > INT_MAX)
            wr_count = INT_MAX;

#ifdef ROMIO_GPFS
	if (gpfsmpio_devnullio)
	    err = pwrite(fd->null_fd, p, wr_count, offset+bytes_xfered);
	else
#endif
        err = pwrite2pmem(fd, fd->fd_sys, p, wr_count, offset + bytes_xfered);
	    // err = pwrite(fd->fd_sys, p, wr_count, offset+bytes_xfered);
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
#ifdef ADIOI_MPE_LOGGING
	MPE_Log_event( ADIOI_MPE_write_b, 0, NULL );
#endif
	bytes_xfered += err;
	p += err;
    }

#ifdef ROMIO_GPFS
    if (gpfsmpio_timing) gpfsmpio_prof_cw[ GPFSMPIO_CIO_T_POSI_RW ] += (MPI_Wtime() - io_time);
#endif
    fd->fp_sys_posn = offset + bytes_xfered;

    if (file_ptr_type == ADIO_INDIVIDUAL) {
	fd->fp_ind += bytes_xfered; 
    }

#ifdef ROMIO_GPFS
    if (gpfsmpio_timing) gpfsmpio_prof_cw[ GPFSMPIO_CIO_T_MPIO_RW ] += (MPI_Wtime() - io_time);
#endif

#ifdef HAVE_STATUS_SET_BYTES
    /* bytes_xfered could be larger than int */
    if (err != -1 && status) MPIR_Status_set_bytes(status, datatype, bytes_xfered);
#endif

    *error_code = MPI_SUCCESS;
#ifdef AGGREGATION_PROFILE
    MPE_Log_event (5037, 0, NULL);
#endif
}
