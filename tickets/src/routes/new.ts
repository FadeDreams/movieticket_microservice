import express, { Request, Response } from 'express';
import { body } from 'express-validator';
import { requireAuth, validateRequest } from '@fadedreams7org1/common';
import { currentUser } from '@fadedreams7org1/common';
import { Ticket } from '../models/ticket';
//import { RabbitMQService } from "@fadedreams7org1/common";
import { RabbitMQService } from "../utils";


const router = express.Router();
const rabbitService = new RabbitMQService("amqp://localhost", "ticket:create");

//router.post('/api/tickets', currentUser, requireAuth, (req: Request, res: Response) => {
//res.sendStatus(200);
//});

router.post(
  '/api/tickets', currentUser,
  requireAuth,
  [
    body('title').not().isEmpty().withMessage('Title is required'),
    body('price')
      .isFloat({ gt: 0 })
      .withMessage('Price must be greater than 0'),
  ],
  validateRequest,
  async (req: Request, res: Response) => {
    const { title, price } = req.body;

    const ticket = Ticket.build({
      title,
      price,
      userId: req.currentUser!.id,
    });
    await ticket.save();
    //await ticket.save();
    //await ticket.save();
    const produceMessage = await rabbitService.startProducer("ticket:created");
    produceMessage({
      id: ticket.id,
      version: ticket.version,
      title,
      price,
      userId: req.currentUser!.id,
    });
    console.log("ticket created and published to ticket:created", ticket);

    res.status(201).send(ticket);
  }


);
export { router as createTicketRouter };

