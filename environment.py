from database import Database
from benchmark import Benchmark
from state import State
from action import Action
from agent import Agent

import json
import pickle
import os
from config import ENV_SAVE_PATH, USE_CHECKPOINT


class Environment:
    """Enviroment class will provide the available actions and rewards in the current state."""

    def __init__(self):
        # Database instance
        self.db = Database()

        # Benchmark
        self.benchmark = Benchmark("TPCH")

        ## Current rewards dictionary
        self.rewards = dict()

        ## State-rewards file records to dict
        # reward list for plotting
        self.rewards_list = list()
        # reward calculated
        self.rewards_archive = dict()
        # visited states
        self.visited_states = list()

        self.states_info = dict()

    def save(self):
        """Save Enviroment data to config.ENV_SAVE_PATH"""
        os.makedirs(os.path.dirname(ENV_SAVE_PATH), exist_ok=True)
        all_data = {
            "rewards": self.rewards,
            "rewards_list": self.rewards_list,
            "rewards_archive": self.rewards_archive,
            "visited_states": self.visited_states,
            "states_info": self.states_info,
        }
        with open(ENV_SAVE_PATH, "wb") as file:
            pickle.dump(all_data, file)

    def load(self):
        """Load Environment data from config.ENV_SAVE_PATH"""
        print("LOAD ENVIRONMENT DATA")
        with open(ENV_SAVE_PATH, "rb") as file:
            all_data = pickle.loads(file.read())

        self.rewards = all_data["rewards"]
        self.rewards_list = all_data["rewards_list"]
        self.rewards_archive = all_data["rewards_archive"]
        self.visited_states = all_data["visited_states"]
        self.states_info = all_data["states_info"]

    def step(self, action: Action):
        """Execute the action, and get the reward with the new state."""
        action.execute()
        state = State()
        reward = self.get_reward(state)
        return state, reward

    def get_action_space(self, state: State):
        """Return all the actions, whether or not it is executable."""
        action_space = list()

        for table, columns in state.indexes_map.items():
            for column in columns.keys():
                action_space.append(Action(table, column, "CREATE"))
                action_space.append(Action(table, column, "DROP"))
        return action_space

    def get_available_actions(self, state):
        """Return all available actions."""
        available_actions = list()
        for table, columns in state.indexes_map.items():
            for column in columns.keys():
                if state.indexes_map[table][column] == 0:
                    available_actions.append(Action(table, column, "CREATE"))
                else:
                    available_actions.append(Action(table, column, "DROP"))

        return available_actions

    def get_reward(self, state: State):
        """Given state, return reward."""
        # Calculate reward (using benchmark)
        if repr(state) in self.rewards_archive.keys():
            self.rewards[state] = self.rewards_archive[repr(state)]
        else:
            self.rewards[state] = self.benchmark.run()

        # Save reward to archive
        self.rewards_archive[repr(state)] = self.rewards[state]

        # Save reward to list for plotting
        self.rewards_list.append(self.rewards[state])

        # Save visited state
        if state not in self.visited_states:
            self.visited_states.append(state)

        return self.rewards[state]

    def get_state_features(self, state: State):
        state_features = dict()
        state_features["Bias"] = 1.0

        for table, columns in state.indexes_map.items():
            for column in columns.keys():
                state_features[column] = state.indexes_map[table][column]
        return state_features

    def reset(self):
        print("In reset")
        self.db.reset_indexes()
        return State()

    """
        Data files and plots
    """

    def dump_rewards_archive(self):
        with open("data/rewards_archive.json", "w+") as outfile:
            json.dump(self.rewards_archive, outfile)

    def dump_states_info(self):
        with open("data/state_info.json", "w+") as outfile:
            json.dump(self.states_info, outfile)

    def dump_rewards_history(self, rewards):
        with open("data/rewards_history_plot.dat", "w+") as outfile:
            for value in rewards:
                outfile.write(str(value) + "\n")

    def post_episode(self, episode, episode_reward, episode_duration, episode_mse):
        # Dump rewards archive
        self.dump_rewards_archive()

        # Dump states info
        self.dump_states_info()

        # Dump computed state-rewards up to now
        self.dump_rewards_history(self.rewards_list)

        # Write episode rewards to file
        with open("data/episode_reward.dat", "a+") as f:
            f.write(str(episode) + ", " + str(episode_reward) + "\n")

        # Write episode duration to file
        with open("data/episode_duration.dat", "a+") as f:
            f.write(str(episode) + ", " + str(episode_duration) + "\n")

        # Write episode MSE to file
        with open("data/episode_mse.dat", "a+") as f:
            f.write(str(episode) + ", " + str(episode_mse) + "\n")

        # Write number of visited distinct states
        with open("data/visited_distinct_states.dat", "a+") as f:
            f.write(str(episode) + ", " + str(len(self.visited_states)) + "\n")

        # Write highest state reward to file
        max_reward = max(self.rewards, key=lambda x: self.rewards.get(x))
        with open("data/max_reward.dat", "a+") as outfile:
            outfile.write(
                str(episode)
                + ", "
                + repr(max_reward)
                + ", "
                + str(self.rewards[max_reward])
                + "\n"
            )


if __name__ == "__main__":
    print("In ENV MAIN METHOD")

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
