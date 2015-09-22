from jupyterhub.spawner import Spawner

def qstat_t(jobid, column):
    """
    Call qstat -t and extract information about a job.

    Parameters
    ----------

    jobid : `int`
        The numeric ID of the job to search for.

    column : `string`
        The name of the column to extract the information about, can be
        "host" or "state".

    Returns
    -------

    result : `string`
        The value of the column, or None if the job can not be found
    """
    qstat_columns = {'state': 4, 'host': 7}
    ret = subprocess.run(['qstat', '-t'], stdout=subprocess.PIPE)

    jobinfo = ret.stdout.decode('utf-8')

    state = None
    for line in jobinfo.split('\n'):
        if line.startswith('{}'.format(jobid)):
            state = line.split()[qstat_columns[column]]

    return state

def qdel(jobid):
    """
    Call qdel on given jobid and stop the process in the queue.

    Parameters
    ----------

    jobid : `int`
        The jobID to kill.
    """
    return subprocess.run(['qdel', '{}'.format(jobid)])

class SGESpawner(Spawner):

    def load_state(self, state):
        super(SGESpawner, self).load_state(state)
        if 'jobid' in state:
            self.jobid = state['jobid']

    def get_state(self):
        state = super(SGESpawner, self).get_state()
        if self.jobid:
            state['jobid'] = self.jobid
        return state

    def clear_state(self):
        super(SGESpawner, self).clear_state()
        self.jobid = None

    @gen.coroutine
    def start(self):
        """
        Submit the job to the queue and wait for it to start
        """
        self.user.server.port = random_port()
        cmd = ['qsub']
        cmd.extend(self.cmd)
        cmd.extend(self.get_args())

        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        r = self.proc.stdout.decode('utf-8')
        jid = int(r.split('Your job ')[1].split()[0])
        self.jobid = jid

        state = qstat_t(jid,'state')
        while state != 'r':
            time.sleep(1)
            state = qstat_t(jid, 'state')

        host = qstat_t(jid, 'host').split('@')[1].split('.')[0]
        self.user.server.ip = host

    @gen.coroutine
    def stop(self, now=False):
        if self.jobid:
            subprocess.Popen(['qdel', '{}'.format(self.jobid)])

    @gen.coroutine
    def poll(self):
        state = qstat_t(self.jobid, 'state')
        if state:
            if state == 'r':
                return None
            else: # qw is not an option here.
                return 1
        else:
            return 0

