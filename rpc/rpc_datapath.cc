#include "libhrd/hrd.h"
#include "rpc/rpc.h"
#include "rpc/rpc_defs.h"
#include "util/rte_memcpy.h"

/* Send a batch of >= 0 requests. Return 0 on success. */
int Rpc::send_reqs(int coro_id)
{
#if RPC_MSR_MAX_BATCH_LATENCY == 1
	/* Record the start time of this request batch */
	clock_gettime(CLOCK_REALTIME, &max_batch_lat_start[coro_id]);
#endif

	rpc_dassert(coro_id > 0 && coro_id < info.num_coro);

	rpc_req_batch_t *req_batch = &req_batch_arr[coro_id];

	int num_uniq_mn = req_batch->num_uniq_mn;

	rpc_dassert(num_uniq_mn >= 1 && num_uniq_mn <= RPC_MAX_MSG_CORO); //DAM -  Max outstanding messages per slave coroutine. 

	stat_num_creqs += num_uniq_mn;

	/* Bookkeeping */
	int wr_i = 0;
	struct ibv_send_wr *bad_wr;

	for(int msg_i = 0; msg_i < num_uniq_mn; msg_i++) {
		rpc_cmsg_t *cmsg = &req_batch->cmsg_arr[msg_i];
#if RPC_DEBUG_ASSERT == 1
		check_coalesced_msg(cmsg->req_mbuf.alloc_buf, cmsg->num_centry);
#endif
		int resp_mn = cmsg->remote_mn;

		/* Verify constant @sgl and @wr fields */
		rpc_dassert(send_sgl[wr_i].lkey == lkey);
		rpc_dassert(send_wr[wr_i].next == &send_wr[wr_i + 1]); /* +1 is valid */
		rpc_dassert(send_wr[wr_i].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
		rpc_dassert(send_wr[wr_i].opcode == IBV_WR_SEND_WITH_IMM);
		rpc_dassert(send_wr[wr_i].num_sge == 1);
		rpc_dassert(send_wr[wr_i].sg_list == &send_sgl[wr_i]);

		/* Encode variable fields */
		send_sgl[wr_i].length = cmsg->req_mbuf.length();
		rpc_dassert(send_sgl[wr_i].length <= info.max_pkt_size);
		send_sgl[wr_i].addr = (uint64_t) cmsg->req_mbuf.alloc_buf;

		send_wr[wr_i].wr.ud.ah = ah[resp_mn];
		send_wr[wr_i].wr.ud.remote_qpn = rem_qpn[resp_mn];
		send_wr[wr_i].send_flags = set_flags(active_qp, send_sgl[wr_i].length);

		/* Encode immediate */
		union rpc_imm imm;
		imm.is_req = 1;
		imm.num_reqs = cmsg->num_centry;
		imm.mchn_id = info.machine_id;	/* This machine's ID */
		imm.coro_id = coro_id;
		imm.config_id = 0;	/* XXX */
		check_imm(imm);	/* Sanity check other fields */

		send_wr[wr_i].imm_data = imm.int_rep;	/* Copy int representation */

		rpc_dprintf("Rpc: Worker %d, coro %d sending request (batch) to "
			"machine %d via QP %d, size = %d\n",
			info.wrkr_gid, coro_id, resp_mn, active_qp, send_sgl[wr_i].length);

		wr_i++;

		/* Actually send requests. @wr_i = total number of messages assembled */
		if(wr_i == info.postlist || msg_i == num_uniq_mn - 1) {
			rpc_dassert(wr_i > 0 && wr_i <= RPC_MAX_POSTLIST); /* Need > 0 */
			send_wr[wr_i - 1].next = NULL;	/* Breaker of chains */

			int ret = ibv_post_send(cb->dgram_qp[active_qp],
				&send_wr[0], &bad_wr);
			rpc_dassert_msg(ret == 0, "Rpc: ibv_post_send error\n");
			rpc_stat_inc(stat_resp_post_send_calls, 1);

			/* Reset */
			send_wr[wr_i - 1].next = &send_wr[wr_i]; /* Restore chain; safe. */
			wr_i = 0;	/* Reset to start of wr array */

			HRD_MOD_ADD(active_qp, info.num_qps);
			/* XXX: Should we calculate post_send() postlist stats here */
		}
	}

	return 0;	/* Success, even though we don't have a fail case */
}



int Rpc::send_reqs(int coro_id, bool _dam)
{
#if RPC_MSR_MAX_BATCH_LATENCY == 1
	/* Record the start time of this request batch */
	clock_gettime(CLOCK_REALTIME, &max_batch_lat_start[coro_id]);
#endif

	rpc_dassert(coro_id > 0 && coro_id < info.num_coro);

	rpc_req_batch_t *req_batch = &req_batch_arr[coro_id];

	int num_uniq_mn = req_batch->num_uniq_mn;
	rpc_dassert(num_uniq_mn >= 1 && num_uniq_mn <= RPC_MAX_MSG_CORO);
	stat_num_creqs += num_uniq_mn;

	/* Bookkeeping */
	int wr_i = 0;
	struct ibv_send_wr *bad_wr;

	for(int msg_i = 0; msg_i < num_uniq_mn; msg_i++) {
		rpc_cmsg_t *cmsg = &req_batch->cmsg_arr[msg_i];
#if RPC_DEBUG_ASSERT == 1
		check_coalesced_msg(cmsg->req_mbuf.alloc_buf, cmsg->num_centry);
#endif
		int resp_mn = cmsg->remote_mn;

		/* Verify constant @sgl and @wr fields */
		rpc_dassert(send_sgl[wr_i].lkey == lkey);
		rpc_dassert(send_wr[wr_i].next == &send_wr[wr_i + 1]); /* +1 is valid */
		rpc_dassert(send_wr[wr_i].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
		rpc_dassert(send_wr[wr_i].opcode == IBV_WR_SEND_WITH_IMM);
		rpc_dassert(send_wr[wr_i].num_sge == 1);
		rpc_dassert(send_wr[wr_i].sg_list == &send_sgl[wr_i]);

		/* Encode variable fields */
		send_sgl[wr_i].length = cmsg->req_mbuf.length();
		rpc_dassert(send_sgl[wr_i].length <= info.max_pkt_size);
		send_sgl[wr_i].addr = (uint64_t) cmsg->req_mbuf.alloc_buf;

		send_wr[wr_i].wr.ud.ah = ah[resp_mn];
		send_wr[wr_i].wr.ud.remote_qpn = rem_qpn[resp_mn];
		send_wr[wr_i].send_flags = set_flags(active_qp, send_sgl[wr_i].length);

		/* Encode immediate */
		union rpc_imm imm;
		imm.is_req = 2; // DAM for delegated requests
		imm.num_reqs = cmsg->num_centry;
		imm.mchn_id = info.machine_id;	/* This machine's ID */
		imm.coro_id = coro_id;
		imm.config_id = 0;	/* XXX */
		check_imm(imm);	/* Sanity check other fields */

		send_wr[wr_i].imm_data = imm.int_rep;	/* Copy int representation */

		rpc_dprintf("Rpc: Worker %d, coro %d sending request (batch) to "
			"machine %d via QP %d, size = %d\n",
			info.wrkr_gid, coro_id, resp_mn, active_qp, send_sgl[wr_i].length);

		wr_i++;

		/* Actually send requests. @wr_i = total number of messages assembled */
		if(wr_i == info.postlist || msg_i == num_uniq_mn - 1) {
			rpc_dassert(wr_i > 0 && wr_i <= RPC_MAX_POSTLIST); /* Need > 0 */
			send_wr[wr_i - 1].next = NULL;	/* Breaker of chains */

			int ret = ibv_post_send(cb->dgram_qp[active_qp],
				&send_wr[0], &bad_wr);
			rpc_dassert_msg(ret == 0, "Rpc: ibv_post_send error\n");
			rpc_stat_inc(stat_resp_post_send_calls, 1);

			/* Reset */
			send_wr[wr_i - 1].next = &send_wr[wr_i]; /* Restore chain; safe. */
			wr_i = 0;	/* Reset to start of wr array */

			HRD_MOD_ADD(active_qp, info.num_qps);
			/* XXX: Should we calculate post_send() postlist stats here */
		}
	}

	return 0;	/* Success, even though we don't have a fail case */
}


/*
 * Send a batch of >= 0 responses. Return 0 on success.
 * Unlike send_reqs(), @batch can contain multiple responses to the same remote
 * worker (but to different coroutines at that worker).
 *
 * Immediate formatting: Copies the request immediate to response immediate, but
 * changes its type to the corresponding response type.
 */
int Rpc::send_resps()
{
	int num_cresps = resp_batch.num_cresps;
	rpc_dassert(num_cresps >= 1 && num_cresps <= HRD_RQ_DEPTH);	/* RECV bound */
	rpc_stat_inc(stat_num_cresps, num_cresps);

	struct ibv_send_wr *bad_wr;

	/* Bookkeeping */
	int wr_i = 0;

	for(int resp_i = 0; resp_i < num_cresps; resp_i++) {
		/* Verify constant fields */
		rpc_dassert(send_sgl[wr_i].lkey == lkey);
		rpc_dassert(send_wr[wr_i].next == &send_wr[wr_i + 1]); /* +1 is valid */
		rpc_dassert(send_wr[wr_i].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
		rpc_dassert(send_wr[wr_i].opcode == IBV_WR_SEND_WITH_IMM);
		rpc_dassert(send_wr[wr_i].num_sge == 1);
		rpc_dassert(send_wr[wr_i].sg_list == &send_sgl[wr_i]);

		rpc_cmsg_t *cmsg = &resp_batch.cmsg_arr[resp_i];
#if RPC_DEBUG_ASSERT == 1
		check_coalesced_msg(cmsg->resp_mbuf.alloc_buf, cmsg->num_centry);
#endif

		int req_mn = cmsg->remote_mn;

		/* Encode variable fields */
		send_wr[wr_i].wr.ud.ah = ah[req_mn];
		send_wr[wr_i].wr.ud.remote_qpn = rem_qpn[req_mn];

		send_sgl[wr_i].addr = (uintptr_t) cmsg->resp_mbuf.alloc_buf;
		send_sgl[wr_i].length = cmsg->resp_mbuf.length();
		rpc_dassert(send_sgl[wr_i].length <= info.max_pkt_size);

		send_wr[wr_i].send_flags = set_flags(active_qp, send_sgl[wr_i].length);

		if(send_sgl[wr_i].length > HRD_MAX_INLINE) {
			rpc_dprintf("Worker %d: Response too large. Using non-inline buf\n",
				info.wrkr_gid);
			uint8_t *ni_buf =
				resp_batch.non_inline_bufs[resp_batch.non_inline_index];
			rte_memcpy((void *) ni_buf, (void *) send_sgl[wr_i].addr,
				send_sgl[wr_i].length);

			send_sgl[wr_i].addr = (uintptr_t) ni_buf;
			HRD_MOD_ADD(resp_batch.non_inline_index, HRD_RQ_DEPTH);
		}

		/* Insert pre-encoded immediate */
		send_wr[wr_i].imm_data = cmsg->resp_imm;

		rpc_dprintf("Rpc: Worker %d sending resp (batch) to machine %d via "
			"QP %d\n", info.wrkr_gid, req_mn, active_qp);

		wr_i++;
		/* wr_i = total number of requests assembled */
		if(wr_i == info.postlist || resp_i == num_cresps - 1) {
			rpc_dassert(wr_i > 0); /* If not, how did we get here? */
			send_wr[wr_i - 1].next = NULL;

			int ret = ibv_post_send(cb->dgram_qp[active_qp],
				&send_wr[0], &bad_wr);
			rpc_dassert_msg(ret == 0, "Rpc: ibv_post_send error\n");

			rpc_stat_inc(stat_resp_post_send_calls, 1);

			/* Reset */
			send_wr[wr_i - 1].next = &send_wr[wr_i]; /* Restore chain; safe */
			wr_i = 0;
			HRD_MOD_ADD(active_qp, info.num_qps);
		}
	}

	return 0;	/* Success, even though we don't have a fail case */
}

void Rpc::ld_check_packet_loss()
{
	struct timespec ld_end;

	clock_gettime(CLOCK_REALTIME, &ld_end);
	double ms = (ld_end.tv_sec - ld_stopwatch.tv_sec) * 1000 +
		(double) (ld_end.tv_nsec - ld_stopwatch.tv_nsec) / 1000000;

	if(ms >= RPC_LOSS_DETECTION_MS) {
		bool pkt_loss = false;

		/*
		 * Print seconds since Rpc initialization so that we can ignore false
		 * positives that occur in the beginning.
		 */
		double seconds_since_init =
			(ld_end.tv_sec - rpc_init_time.tv_sec) +
			(ld_end.tv_nsec - rpc_init_time.tv_nsec) / 1000000000;

		for(int coro_i = 1; coro_i < info.num_coro; coro_i++) {
			if(ld_num_resps_ever[coro_i] ==
					ld_num_resps_ever_prev[coro_i])  { /* No progress => Loss */
				/* Print to screen */
				printf("Worker %d: Packet loss for coro %d. Idle for %.2f ms. "
					"Seconds since init = %.2fs\n",
					info.wrkr_gid, coro_i, ms, seconds_since_init);

				/* Save to loss file */
				fprintf(ld_fp, "Worker %d: Packet loss for coro %d. "
					"Idle for %.2f ms. Seconds since init = %.2fs\n",
					info.wrkr_gid, coro_i, ms, seconds_since_init);
				fflush(ld_fp);

				pkt_loss = true;
			}

			ld_num_resps_ever_prev[coro_i] = ld_num_resps_ever[coro_i];
		}

		/* Print out progress status at worker 0 */
		if(!pkt_loss && info.wrkr_lid == 0) {
			printf("Worker %d: All coroutines made progress for %.2f ms. "
				"Seconds since init = %.2fs\n",
				info.wrkr_gid, ms, seconds_since_init);
		}

		clock_gettime(CLOCK_REALTIME, &ld_stopwatch);
	}
}

coro_id_t* Rpc::poll_comps()
{
	// Loss detection - check if a coroutine has not made progress for too long.
	// This cannot be made dependent on successfully polling a RECV, since we
	// may never get a RECV completion during packet loss.
	ld_iters++;
	if(ld_iters == RPC_LOSS_DETECTION_STEP) {
		if(!is_dam_node()){
			ld_check_packet_loss(); //DAM ommiting
		}
			ld_iters = 0;
	}

	/* Poll for completions */
	long long poll_recv_cq_start = rpc_get_cycles();
	int cq_comps = ibv_poll_cq(cb->dgram_recv_cq[0], HRD_RQ_DEPTH, wc);
	tot_cycles_poll_recv_cq += rpc_get_cycles() - poll_recv_cq_start;

	if(cq_comps == 0) {
		rpc_stat_inc(stat_wasted_poll_cq, 1);

		/* Return a loop with only the master coroutine */
		//rpc_dprintf("Rpc: Return the master coroutine. nothing received");
		next_coro[RPC_MASTER_CORO_ID] = RPC_MASTER_CORO_ID;
		return next_coro;
	}

	/* Reset the response batch if there are completions */
	int cur_comp_coro = RPC_MASTER_CORO_ID;	/* For completed coroutines */
	resp_batch.clear();

#if RPC_ENABLE_MICA_PREFETCH == 1
	for(int comp_i = 0; comp_i < cq_comps; comp_i++) {
		uint8_t *wc_buf = (uint8_t *) (wc[comp_i].wr_id + HOTS_GRH_BYTES);
		union rpc_imm wc_imm;
		wc_imm.int_rep = wc[comp_i].imm_data;
		uint32_t _is_req = wc_imm.is_req;
		uint32_t _num_reqs = wc_imm.num_reqs;

		if(_is_req >= 1) { //prefeching DAM included
			prefetch_mica(wc_buf, _num_reqs);
		}
	}
#endif

	for(int comp_i = 0; comp_i < cq_comps; comp_i++) {
		/* Unmarshal the completion's immediate */
		union rpc_imm wc_imm;
		wc_imm.int_rep = wc[comp_i].imm_data;
		check_imm(wc_imm);;

		uint32_t _is_req = wc_imm.is_req;
		uint32_t _num_reqs = wc_imm.num_reqs;	/* or number of resps */
		uint32_t _mchn_id = wc_imm.mchn_id;	/* Remote machine's ID */
		uint32_t _coro_id = wc_imm.coro_id;
		uint32_t _config_id __attribute__((unused)) = wc_imm.config_id;

		rpc_dassert(_mchn_id < (unsigned) info.num_machines);
		rpc_dassert(_coro_id < (unsigned) info.num_coro);

		/* Interpret the received buffer */
		uint8_t *wc_buf = (uint8_t *) (wc[comp_i].wr_id + HOTS_GRH_BYTES);
		rpc_dassert(is_aligned(wc_buf, 64));

#if RPC_DEBUG_ASSERT == 1
		check_coalesced_msg(wc_buf, _num_reqs);
#endif

		/* wc.byte_len includes GRH, whether or not GRH is DMA-ed */
		size_t wc_len = wc[comp_i].byte_len - HOTS_GRH_BYTES;
		rpc_dassert(wc_len >= 0 && wc_len <= info.max_pkt_size);

		if(_is_req == 0) {
			// Handle a new response
			rpc_dassert(_coro_id != RPC_MASTER_CORO_ID);

			rpc_req_batch_t *req_batch = &req_batch_arr[_coro_id];
			req_batch->num_reqs_done += _num_reqs;

			if(req_batch->num_reqs_done == req_batch->num_reqs) {
				/* Record completed coroutine */
				rpc_dprintf("Rpc: Worker %d received all responses "
					"for coroutine %d\n", info.wrkr_gid, _coro_id);

				next_coro[cur_comp_coro] = _coro_id;
				cur_comp_coro = _coro_id;

#if RPC_MSR_MAX_BATCH_LATENCY == 1
				/*
				 * Save the latency of the completed batch if it exceeds the
				 * current maximum batch latency for this coroutine.
				 */
				struct timespec *_mbl_start = &max_batch_lat_start[_coro_id];
				struct timespec _mbl_end;
				clock_gettime(CLOCK_REALTIME, &_mbl_end);
				double _mbl_usec =
					(_mbl_end.tv_sec - _mbl_start->tv_sec) * 1000000 +
					(double) (_mbl_end.tv_nsec - _mbl_start->tv_nsec) / 1000;

				if(_mbl_usec > max_batch_lat_coro[_coro_id]) {
					max_batch_lat_coro[_coro_id] = _mbl_usec;
				}
#endif
			}

			/* Fill in the slave coroutines's response buffers */
			size_t wc_off = 0;	/* Byte index into the coalesced resp */
			rpc_cmsg_reqhdr_t *cmsg_reqhdr;
			for(int i = 0; i < (int) _num_reqs; i++) {
				rpc_dassert(is_aligned(wc_off, sizeof(rpc_cmsg_reqhdr_t)));
				cmsg_reqhdr = (rpc_cmsg_reqhdr_t *) &wc_buf[wc_off];

				rpc_dprintf("Rpc: Worker %d received %s response from "
					"machine %d. Size = %u (coalesced size = %lu) for coroutine- %d\n",
					info.wrkr_gid,
					rpc_type_to_string(cmsg_reqhdr->resp_type).c_str(),
					_mchn_id, cmsg_reqhdr->size, wc_len, _coro_id);

				//rpc_dassert(cmsg_reqhdr->resp_type != RPC_LOCKSERVER_REQ); //DAM assert to check RPC_LOCKSERVER_REQ

				/* Unmarshal the request header */
				rpc_dassert(cmsg_reqhdr->magic == RPC_CMSG_REQ_HDR_MAGIC);
				int _coro_seqnum = cmsg_reqhdr->coro_seqnum;
				rpc_dassert(_coro_seqnum >= 0 &&
					_coro_seqnum <= req_batch->num_reqs);

				/* Copy the response data to the user's buffer */
				rpc_req_t *req = &req_batch->req_arr[_coro_seqnum];

				rpc_dassert(req->max_resp_len >= cmsg_reqhdr->size); //DAM- responses mismatch

				rte_memcpy(req->resp_buf,
					&wc_buf[wc_off + sizeof(rpc_cmsg_reqhdr_t)],
					cmsg_reqhdr->size);

				/* Copy response metadata */
				req->resp_len = cmsg_reqhdr->size;
				req->resp_type = cmsg_reqhdr->resp_type;

				wc_off += sizeof(rpc_cmsg_reqhdr_t) + cmsg_reqhdr->size;
			}
			rpc_dassert(wc_off == wc_len);

			/* Record coroutine progress for packet loss detection */
			ld_num_resps_ever[_coro_id] += _num_reqs;
		} else {

			// Handle a new request
			rpc_dassert(wc_len > 0);	/* Requests cannot be 0-byte */

			if(_is_req == 1){
				
					/* Initialize a new RPC message for the master */
				hots_mbuf_t *resp_mbuf = start_new_resp(_mchn_id, _num_reqs,
					wc_imm.int_rep); /* Converts req Imm to response Imm */
	
				/* Process the requests */
				size_t wc_off = 0;	/* Offset into wc_buf */
				rpc_cmsg_reqhdr_t *cmsg_reqhdr;
	
				//YALA: need to get all the requests and create readset and writset.
	
				for(int i = 0; i < (int) _num_reqs; i++) {
					rpc_dassert(is_aligned(wc_off, sizeof(rpc_cmsg_reqhdr_t)));
					rpc_dassert(is_aligned(resp_mbuf->cur_buf,
						sizeof(rpc_cmsg_reqhdr_t)));
	
					/* Unmarshal the request header */
					cmsg_reqhdr = (rpc_cmsg_reqhdr_t *) &wc_buf[wc_off];
					rpc_dassert(cmsg_reqhdr->magic == RPC_CMSG_REQ_HDR_MAGIC);
					uint32_t req_type = cmsg_reqhdr->req_type;
					uint32_t req_len = cmsg_reqhdr->size;
	
					rpc_dprintf("Rpc: Worker %d received %s req from machine %d and coro id - %d. "
						"Size = %u (coalesced size = %lu, %d reqs)\n",
						info.wrkr_gid,
						rpc_type_to_string(cmsg_reqhdr->req_type).c_str(),
						_mchn_id, _coro_id, cmsg_reqhdr->size, wc_len, (int) _num_reqs);

					//rpc_dassert(cmsg_reqhdr->req_type != RPC_LOCKSERVER_REQ); //DAM to check the lockerserver issue.
	
					/* Copy the request header to the response */
					rpc_cmsg_reqhdr_t *cmsg_resphdr = (rpc_cmsg_reqhdr_t *)
						resp_mbuf->cur_buf;
					*((uint64_t *) cmsg_resphdr) = *(uint64_t *) cmsg_reqhdr; //copy
					resp_mbuf->cur_buf += sizeof(rpc_cmsg_reqhdr_t);
					wc_off += sizeof(rpc_cmsg_reqhdr_t);
	
					/* Invoke the handler */
					rpc_dassert(rpc_handler[req_type] != NULL);
	
					size_t resp_len = rpc_handler[req_type](
						resp_mbuf->cur_buf, &cmsg_resphdr->resp_type,
						&wc_buf[wc_off], req_len, rpc_handler_arg[req_type]); //
	
					cmsg_resphdr->size = resp_len;	/* cmsg_resphdr is valid */
	
					rpc_dassert(is_aligned(resp_len, sizeof(uint64_t)));
	
					wc_off += req_len;
					resp_mbuf->cur_buf += resp_len;
	
					/* Ensure that we don't overflow the response buffer */
					rpc_dassert(resp_mbuf->length() <= resp_mbuf->alloc_len);
				}
	
				rpc_dassert(wc_off == wc_len);	


			}

			//is_req=2
			else{ //DAM delegate request. first lock and then commit out of the critical path.

				/* Initialize a new RPC message for the master */
				hots_mbuf_t *resp_mbuf = start_new_resp(_mchn_id, _num_reqs,
					wc_imm.int_rep); /* Converts req Imm to response Imm */
	
				 /* YALA only single respose is enough
				    hots_mbuf_t *resp_mbuf = start_new_resp(_mchn_id, 1,
					wc_imm.int_rep);
		    	*/
	
				/* Process the requests */
				size_t wc_off = 0;	/* Offset into wc_buf */		
				rpc_cmsg_reqhdr_t *cmsg_reqhdr;	
				bool tx_failed=false;
	
				//YALA: need to get all the requests and create readset and writset. next lock them.    
				//Step-1: RTT to lock all the places and get the response.
            	for(int i = 0; i < (int) _num_reqs; i++) {
					rpc_dassert(is_aligned(wc_off, sizeof(rpc_cmsg_reqhdr_t)));
					rpc_dassert(is_aligned(resp_mbuf->cur_buf,
						sizeof(rpc_cmsg_reqhdr_t)));
	
					/* Unmarshal the request header */
					cmsg_reqhdr = (rpc_cmsg_reqhdr_t *) &wc_buf[wc_off];
					rpc_dassert(cmsg_reqhdr->magic == RPC_CMSG_REQ_HDR_MAGIC);
					uint32_t req_type = cmsg_reqhdr->req_type;
					uint32_t req_len = cmsg_reqhdr->size;
	
					rpc_dprintf("Rpc: Worker %d received %s req from machine %d coro id - %d. "
						"Size = %u (coalesced size = %lu, %d reqs)\n",
						info.wrkr_gid,
						rpc_type_to_string(cmsg_reqhdr->req_type).c_str(),
						_mchn_id, _coro_id, cmsg_reqhdr->size, wc_len, (int) _num_reqs);
	
	
					// DAM only the first or the last message of the batch need to be attached with the respose.
					// make sure that only one transaction from one machine is processed at one. batch can consis tof multiple batches. highly unlikely?.
	
					/* Copy the request header to the response */
					rpc_cmsg_reqhdr_t *cmsg_resphdr = (rpc_cmsg_reqhdr_t *)	resp_mbuf->cur_buf;
					*((uint64_t *) cmsg_resphdr) = *(uint64_t *) cmsg_reqhdr; //copy
					resp_mbuf->cur_buf += sizeof(rpc_cmsg_reqhdr_t);
					wc_off += sizeof(rpc_cmsg_reqhdr_t);
	
					//DAM
					/*recreate the read-set and write-set*/	
					/*invoke a handler for the transaction which does the two phase locking.
					(logging and replication can be used in case memory fails fully or partially. for fully failure logless protocols can be used)*/
	    				//change the RPC layer to send and proecss transaction as a whole
	    				//send a number of items in the transactions to here and then we can chekc if all the keys have arrived.
    				//
	
					/* Invoke the handler */
					rpc_dassert(rpc_handler[req_type] != NULL);
	
					//DAM only need to get the respose and accumulate them. slave coroutines on the memory side can be used.
					//single master may not enough.
	
	
					// changed the use cases to match get_for_upd and lock_for_ins	
					size_t resp_len = rpc_handler[req_type](
						resp_mbuf->cur_buf, &cmsg_resphdr->resp_type,
						&wc_buf[wc_off], req_len, rpc_handler_arg[req_type]); //
	
					
					//cmsg_resphdr->size = resp_len;	/* cmsg_resphdr is valid */
					//DAM reducing the response size
					cmsg_resphdr->size = 0;	/* cmsg_resphdr is valid */

					if(resp_len==0) tx_failed=true;
	
					//YALA - iterate over.we can send the respose to the first or the last request of the packet
	
					rpc_dassert(is_aligned(resp_len, sizeof(uint64_t)));
	
					wc_off += req_len;
					//resp_mbuf->cur_buf += resp_len;
					//DAM reducing the response size
					resp_mbuf->cur_buf += 0;

	
					/* Ensure that we don't overflow the response buffer */
					rpc_dassert(resp_mbuf->length() <= resp_mbuf->alloc_len);
				
				}// First round of locking is done.				
            	

				//if(tx_failed) continue; // still need to unlock rd_only and wrd-wr set. if validation can be done during the first round, no need to lock those rd_only data.
				
				//DAM sending early results to the coordinator!.
				
				send_resps(); //DAM - issue sending responses twise without clearing the buffer.
				resp_batch.clear(); // no more dublicate messages are sent to the cline side.

				//Step-2 ; compare and unlocking and updating put/insert values. 
				// if the transaction fails, only need to unlock. no updates. 
				rpc_dprintf("Rpc: Worker %d transaction status : %d from machine %d and coro is - %d\n",
						info.wrkr_gid, (tx_failed?0:1), _mchn_id, _coro_id);	

				//dummy reponse buffer
				//rpc_cmsg_t dam_cmsg; //unused
				//resp_mbuf dam_resp_mbuf;
				//dam_resp_mbuf.alloc_len(info.max_pkt_size);

            	wc_off = 0;	

            	hots_mbuf_t *dam_resp_mbuf;
				dam_resp_mbuf = (hots_mbuf_t*)malloc(sizeof(hots_mbuf_t)); 
				dam_resp_mbuf->alloc(info.max_pkt_size); //dummy response buffer.
            	//should happen out of the cricitcal path. 

            	//DAM - check null pointers here. probably an error or memory overflowing.
            	rpc_dassert(dam_resp_mbuf !=NULL); 
            	rpc_dassert(dam_resp_mbuf->cur_buf != NULL);

				for(int i = 0; i < (int) _num_reqs; i++) {
										
					rpc_dprintf("request num  - %d \n", (int) _num_reqs);

					rpc_dassert(is_aligned(wc_off, sizeof(rpc_cmsg_reqhdr_t)));
					rpc_dassert(is_aligned(&dam_resp_mbuf,sizeof(rpc_cmsg_reqhdr_t)));
	
					/* Unmarshal the request header */
					cmsg_reqhdr = (rpc_cmsg_reqhdr_t *) &wc_buf[wc_off];
					rpc_dassert(cmsg_reqhdr->magic == RPC_CMSG_REQ_HDR_MAGIC);
					uint32_t req_type = cmsg_reqhdr->req_type;
					uint32_t req_len = cmsg_reqhdr->size;
	
					rpc_dprintf("Rpc: Worker %d received %s req from machine %d. "
						"Size = %u (coalesced size = %lu, %d reqs)\n",
						info.wrkr_gid,
						rpc_type_to_string(cmsg_reqhdr->req_type).c_str(),
						_mchn_id, cmsg_reqhdr->size, wc_len, (int) _num_reqs);	
	
					// DAM only the first or the last message of the batch need to be attached with the respose.
					// make sure that only one transaction from one machine is processed at one. batch can consist of multiple batches. highly unlikely?.
	
					/* Copy the request header to the response */
					rpc_cmsg_reqhdr_t *cmsg_resphdr = (rpc_cmsg_reqhdr_t *)
						dam_resp_mbuf->cur_buf;
					*((uint64_t *) cmsg_resphdr) = *(uint64_t *) cmsg_reqhdr; //copy
					dam_resp_mbuf->cur_buf += sizeof(rpc_cmsg_reqhdr_t);

					wc_off += sizeof(rpc_cmsg_reqhdr_t);
	
					//DAM
					/*recreate the read-set and write-set*/
	
					/*invoke a handler for the transaction which does the two phase locking.
					(logging and replication can be used in case memory fails fully or partially. for fully failure logless protocols can be used)*/
	
    				//change the RPC layer to send and proecss transaction as a whole
	
    				//send a number of items in the transactions to here and then we can chekc if all the keys have arrived.
    				//
	
					/* Invoke the handler */
					rpc_dassert(rpc_handler[req_type] != NULL);
	
					//YALA only need to get the respose and accumulate them. slave coroutines on the memory side can be used.
					//single master may not enough.
	
					//call the new handler, pass a vector.

					ds_generic_get_req_t *_req = (ds_generic_get_req_t *) &wc_buf[wc_off];
					ds_reqtype_t _req_type = static_cast<ds_reqtype_t>(_req->req_type);

					if(tx_failed){
						rpc_dprintf("Transaction has failed. worker : %d \n", info.wrkr_gid);

						switch(_req_type){
							case ds_reqtype_t::get_rdonly_dam: {
								_req->req_type = (uint64_t) ds_reqtype_t::unlock;

								break;
							}
							case ds_reqtype_t::put_dam:{
								_req->req_type = (uint64_t) ds_reqtype_t::unlock;
								break;
							}
							case ds_reqtype_t::insert_dam:{
								_req->req_type = (uint64_t) ds_reqtype_t::unlock;
								break;
							}
							case ds_reqtype_t::del_dam:{
								_req->req_type = (uint64_t) ds_reqtype_t::unlock;
								break;
							}
							default:{
								break;
							}	
						}
					}
					else{
						rpc_dprintf("Transaction is SUccessful. worker : %d \n", info.wrkr_gid);
						switch(_req_type){

							//need to match the read version.
							case ds_reqtype_t::get_rdonly_dam: {
								_req->req_type = (uint64_t) ds_reqtype_t::unlock;
								//read version comparison can be done in the first round trip. 
								break;
							}
							case ds_reqtype_t::put_dam:{
								_req->req_type = (uint64_t) ds_reqtype_t::put;
								break;
							}
							case ds_reqtype_t::insert_dam:{
								_req->req_type = (uint64_t) ds_reqtype_t::put; // insert is put when lock succeed.
								break;
							}
							case ds_reqtype_t::del_dam:{
								_req->req_type = (uint64_t) ds_reqtype_t::del;
								break;
							}
							default:{

							}
						}

					}

					rpc_dprintf("Set request type %d \n", (int) _req->req_type );

					//cannot use the same respone buffer as the first one.


					size_t resp_len = rpc_handler[req_type](
						dam_resp_mbuf->cur_buf, &cmsg_resphdr->resp_type,
						&wc_buf[wc_off], req_len, rpc_handler_arg[req_type]); //
	
					cmsg_resphdr->size = resp_len;	/* cmsg_resphdr is valid */
	
					//DAM - Iterate over. we can send the respose to the first or the last request of the packet
	
					rpc_dassert(is_aligned(resp_len, sizeof(uint64_t)));
	
					wc_off += req_len;
					dam_resp_mbuf->cur_buf += resp_len;
	
					/* Ensure that we don't overflow the response buffer */
					rpc_dassert(dam_resp_mbuf->length() <= dam_resp_mbuf->alloc_len);
				}
					//DAM deallocating memory to avoid memory overflowing.
					dam_resp_mbuf->free_mbuf();
					free(dam_resp_mbuf);

					rpc_dassert(wc_off == wc_len);	

					rpc_dprintf("End delegated request %d \n", _coro_id);

			}//DAM delegate request end

			//we can do comit validate and a;l the steps here.
			//careful with memory access timing for locking and validate. becuase its not synchronous in ASICS.

		}
	}

	/*
	 * Post new RECVs. At this point, we do not need the polled buffers
	 * anymore because (a) the polled requests have been processed by the
	 * master and results have been stored into corresponding @resp_buf's,
	 * and (b) the polled responses have been copied to slave coroutine
	 * @resp_buf's.
	 */
	recvs_to_post += cq_comps;
	if(recvs_to_post >= recv_slack) {
		long long post_recv_start = rpc_get_cycles();
#if RPC_ENABLE_MODDED_DRIVER == 1
		post_recvs_fast(recvs_to_post);
#else
		post_recvs(recvs_to_post);
#endif
		recvs_to_post = 0;
		tot_cycles_post_recv += rpc_get_cycles() - post_recv_start;
	}

	if(resp_batch.num_cresps > 0) {
		send_resps();
	}

	next_coro[cur_comp_coro] = RPC_MASTER_CORO_ID;	/* Create the loop */

#if RPC_DEBUG_ASSERT == 1
	/* Completed coro list cannot have duplicates, and must form a loop. */
	int occurences[RPC_MAX_CORO] = {0};
	int _c = next_coro[RPC_MASTER_CORO_ID];
	int _steps = 0;
	while(_c != RPC_MASTER_CORO_ID) {
		if(occurences[_c] == 1) {
			rpc_dassert_msg(false, "Rpc: Duplicates in completed coroutines!");
		}
		occurences[_c] = 1;	/* Record occurence of this coroutine */

		_steps++;
		if(_steps >= 2 * info.num_coro) {
			rpc_dassert_msg(false, "Rpc: Completed coroutine list too long!");
		}

		_c = next_coro[_c];
	}
#endif

	return next_coro;
}

