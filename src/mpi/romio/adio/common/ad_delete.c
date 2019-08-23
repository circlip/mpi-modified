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

void ADIOI_GEN_Delete(const char *filename, int *error_code)
{
    int err;
    static char myname[] = "ADIOI_GEN_DELETE";

    /* TODO: remove pmem file */

    char pmemfilename[1024];
    sprintf(pmemfilename, "%s/%s_pmem", externprefix, filename);
    /* if pmemfile exists */
    if (access(pmemfilename, F_OK) == 0) {
        struct pmemfileinfo pfi;
        int tmpfd = open(pmemfilename, O_RDONLY);
        int ret = pread(tmpfd, &pfi, sizeof(struct pmemfileinfo), 0);
        close(tmpfd);

        /* release data blk and free objs */
        size_t filesize = D_RO(pfi.stat)->st_size;
        int blkcount = (filesize - 1) / PMEMblk_SIZE + 1;
        for (int i = 0; i < blkcount; i++) {
            if (D_RO(pfi.data)[i] != 0) {
                pmemblk_release(D_RO(pfi.data)[i]);
            }
        }

        POBJ_FREE(&(pfi.lock));
        POBJ_FREE(&(pfi.update));
        POBJ_FREE(&(pfi.data));
        POBJ_FREE(&(pfi.stat));
        POBJ_FREE(&(pfi.offsets));
        POBJ_FREE(&(pfi.counts));
        POBJ_FREE(&(pfi.ranks));

        unlink(pmemfilename);
    }

    /* TODO: modified above */

    err = unlink(filename);
    if (err == -1) {
	*error_code = ADIOI_Err_create_code(myname, filename, errno);
    }
    else *error_code = MPI_SUCCESS;
}
