from agent import Agent
from environment import Environment

from config import USE_CHECKPOINT


if __name__ == "__main__":
    env = Environment()
    agent = Agent()
    agent.env = env
    if not USE_CHECKPOINT:
        agent.state = env.reset()
        print("env resetted")
        agent.weights_initialization(agent.state)
        print("Self weigth initialized")
    else:
        agent.env.load()
        agent.load()

    agent.train()