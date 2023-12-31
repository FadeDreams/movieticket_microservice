import express from 'express';
//import { currentUser } from '../middlewares/current-user';
//import { requireAuth } from '../middlewares/require-auth';

import { currentUser } from '@fadedreams7org1/common';
import { requireAuth } from '@fadedreams7org1/common';

const router = express.Router();

router.get('/api/users/currentuser', currentUser, requireAuth, (req, res) => {
  //router.get('/api/users/currentuser', currentUser, (req, res) => {
  console.log(req.session);
  res.send({ currentUser: req.currentUser || null });
});

export { router as currentUserRouter };
