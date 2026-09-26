Your application must survive **>10 000 requests per second**, with potentially
millions of records in the database. Use **MongoDB, Redis, or another MPP / NoSQL
OLTP store**.

Deliver **Python code** with functions to:

1. **NoSQL base performance** new records for some entity (some collection). Simple performance test - get RPS for NoSQL.
2. **OLTP performance** a python function for classical SQL database, not a single data operation, but a sequence of 2-3 operations. Try to guess the performance, measure real values, try to explain bottle-necks and improvements. Test by few 100 of launches.
3. **NoSQL Performance boost** Take function from part 2, speed it up by switching some operations to NoSQL. Show performance boost.
4. **NoSQL Show me what you gave up.** You moved this workload out of your ACID database
   for a reason, and you paid for it. **Demonstrate a stale read**, a lost write, a
   dirty counter, or an eventual-consistency window — in code, visibly. Do it by emulating crash (probabalistic exploding code inside a function) or some other option.

**Optional**
5. **Explain why it is good.** Explain in `ADR.md` why what is given up is acceptable *for this particular entity of your business*
   and would not be acceptable for another one.
   ("We use Redis because it is fast" is not a design decision. "We use Redis for
   the view counter because losing 200 milliseconds of counts during a failover
   costs the business nothing, whereas losing a payment does" — that is a design
   decision.)

**Viva questions you should expect:** *"Which of your entities could you NOT have
moved here, and why?" "It's 3 a.m. and the cache is empty — what happens to your
Postgres?"*
