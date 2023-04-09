from tqdm import tqdm
import random, pprint, time
import pickle
from config import AGENT_SAVE_FILE
import os


class Agent:
    """Q-learning agent

    Update formula:
    Q(s, a) = Q(s, a) + \alpha * (reward + \gamma \max_a Q(s', a) - Q(s, a))
    where s is the current state, a is the selected action, s' is the next state

    Storing a dictionary Q maps (state, action) to values costs a lot of memory.
    We use an approxiamate function to compute the Q-value. Assume there are serveral features
    in State. In our case, features are the columns in the database, if column i is indexed,
    features[i] = 1, otherwise, features[i] = 0
    Q(s, a) = f(state, action) = action_weights[action] + \sum_i features[i] * feature_weights[i]
    """

    MAX_TRAINING_EPISODES = 10
    MAX_STEPS_PER_EPISODE = 100

    def __init__(self):
        self.env = None
        # Stats attributes
        self.episode_reward = dict()
        self.episode_duration = dict()
        self.episode_mse = dict()

        # Agent attributes
        self.state = None
        self.next_state = None
        self.reward = 0.0
        self.action = None

        self.alpha = 0.01  # Learning rate
        self.gamma = 0.8  # Discount factor
        self.epsilon = 0.9  # Exploration probability

        # action to weight
        self.action_weights = dict()
        # feature column to weight
        self.feature_weights = dict()

        self.frozen_action_weights = dict()
        self.frozen_feature_weights = dict()

        self.replay_memory = list()

        self.finished_episode_num = 0
        self.finished_step_num = 0

    def save(self):
        """Save Agent data to config.AGENT_SAVE_FILE"""
        os.makedirs(os.path.dirname(AGENT_SAVE_FILE), exist_ok=True)
        all_data = {
            "episode_reward": self.episode_reward,
            "episode_duration": self.episode_duration,
            "episode_mse": self.episode_mse,
            "state": self.state,
            "next_state": self.next_state,
            "reward": self.reward,
            "action": self.action,
            "action_weights": self.action_weights,
            "feature_weights": self.feature_weights,
            "frozen_action_weights": self.frozen_action_weights,
            "frozen_feature_weights": self.frozen_feature_weights,
            "replay_memory": self.replay_memory,
            "finished_episode_num": self.finished_episode_num,
            "finished_step_num": self.finished_step_num,
        }
        with open(AGENT_SAVE_FILE, "wb") as file:
            pickle.dump(all_data, file)

    def load(self):
        """Load Agent data from config.AGENT_SAVE_FILE"""
        print("LOAD AGENT DATA")
        with open(AGENT_SAVE_FILE, "rb") as file:
            all_data = pickle.loads(file.read())

        self.episode_reward = all_data["episode_reward"]
        self.episode_duration = all_data["episode_duration"]
        self.episode_mse = all_data["episode_mse"]
        self.state = all_data["state"]
        self.next_state = all_data["next_state"]
        self.reward = all_data["reward"]
        self.action = all_data["action"]
        self.action_weights = all_data["action_weights"]
        self.feature_weights = all_data["feature_weights"]
        self.frozen_action_weights = all_data["frozen_action_weights"]
        self.frozen_feature_weights = all_data["frozen_feature_weights"]
        self.replay_memory = all_data["replay_memory"]
        self.finished_episode_num = all_data["finished_episode_num"]
        self.finished_step_num = all_data["finished_step_num"]

    def weights_initialization(self, state):
        """Initial weights"""
        state_features = self.env.get_state_features(self.state)
        action_space = self.env.get_action_space(state)
        for feature in state_features.keys():
            self.feature_weights[feature] = random.random()
        for action in action_space:
            self.action_weights[action] = random.random()

    def predict(self, state, action=None):
        """Compute the approxiamate Q-value
        Q(state, action) = f(state, action) = action_weight[action] + \sum_i features[i] * feature_weights[i]
        """
        state_features = self.env.get_state_features(state)

        if action == None:
            prediction = dict()
            for action, action_weight in self.action_weights.items():
                prediction[action] = action_weight
                for feature, feature_weight in self.feature_weights.items():
                    prediction[action] += feature_weight * state_features[feature]
        else:
            prediction = self.action_weights[action]
            for feature, feature_weight in self.feature_weights.items():
                prediction += feature_weight * state_features[feature]

        return prediction

    def predict_replay(self, state):
        """Compute the approxiamate Q-value with the previous weights"""
        state_features = self.env.get_state_features(state)

        prediction = dict()
        for action, action_weight in self.frozen_action_weights.items():
            prediction[action] = action_weight
            for feature, feature_weight in self.frozen_feature_weights.items():
                prediction[action] += feature_weight * state_features[feature]

        return prediction

    def argmax_a(self, state):
        """argmax_{a in available actions} Q(state, a)"""
        a = None
        max_value = float("-inf")

        q_values = self.predict(state)

        for action in self.env.get_available_actions(state):
            q_value = q_values[action]
            if q_value > max_value:
                max_value = q_value
                a = action

        return a

    def max_a(self, state):
        """max_{a in available actions} Q(state, a)"""
        max_value = float("-inf")

        q_values = self.predict(state)

        for action in self.env.get_available_actions(state):
            q_value = q_values[action]
            if q_value > max_value:
                max_value = q_value

        if max_value == float("-inf"):
            max_value = 0.0

        return max_value

    def max_a_replay(self, state):
        """max_{a in replay} Q(state, a)"""
        max_value = float("-inf")

        q_values = self.predict_replay(state)

        for action in self.env.get_available_actions(state):
            q_value = q_values[action]
            if q_value > max_value:
                max_value = q_value

        if max_value == float("-inf"):
            max_value = 0.0

        return max_value

    def get_random_action(self, state):
        """Choose random action"""
        actions = self.env.get_available_actions(state)
        return random.choice(actions)

    def get_action_epsilon_greedy(self, state):
        """Epsilon-greedy"""
        # Epsilon-greedily choose action
        rand = random.random()

        if rand > self.epsilon:  # EXPLOIT
            # Log action type
            print("*** take ARGMAX action ***")
            action = self.argmax_a(state)
        else:  # EXPLORE
            # Log action type
            print("*** take RANDOM action ***")
            action = self.get_random_action(state)

        return action

    def update(self, state, action, td_target, q_value):
        """Update formula:
        Q(s, a) = Q(s, a) + \alpha * (reward + \gamma \max_a Q(s', a) - Q(s, a))

        Let
            td_target = reward + \gamma \max_a Q(s', a)
            q_value = Q(s, a)

        Update formula can be written in
            Q(s, a) = Q(s, a) + \alpha * (td_target - q_value)

        As we mentioned before, Q(s, a) = action_weights[action] + \sum_i features[i] * feature_weights[i]

        Update formula for action_weights and feature_weights
            action_weights[action] = action_weights[action] + \alpha * (td_target - q_value)
            feature_weights[i] = feature_weights[i] + features[i] * \alpha * (td_target - q_value)
        """
        state_features = self.env.get_state_features(state)
        self.action_weights[action] += self.alpha * (td_target - q_value)

        for weight in self.feature_weights.keys():
            feature = state_features[weight]
            self.feature_weights[weight] += self.alpha * (td_target - q_value) * feature

    def experience_replay(self):
        """Sample a set of memory, and perform update."""
        samples = [random.choice(self.replay_memory) for _ in range(32)]
        # sample = [state, action, reward, next_state]
        for sample in samples:
            # Calculate TD target w.r.t. old frozen weights
            td_target = sample[2] + self.gamma * self.max_a_replay(sample[3])
            # Predict Q-value w.r.t. newest weights
            q_value = self.predict(sample[0], sample[1])
            # Perform gradient descent
            self.update(sample[0], sample[1], td_target, q_value)

    def train(self):
        """training process
        for ... in episode:
            play replay
            for ... in step:
                1. choose an action
                2. execute the action
                3. get the reward
                4. update the weights
        """
        # Episodes loop
        for episode in tqdm(range(self.MAX_TRAINING_EPISODES)):
            # Update statistics
            self.episode_reward[episode] = 0.0
            self.episode_mse[episode] = 0.0
            episode_start_time = time.time()

            # Steps in each episode
            for step in range(self.MAX_STEPS_PER_EPISODE):
                if episode < self.finished_episode_num:
                    continue
                if (
                    episode == self.finished_episode_num
                    and step <= self.finished_step_num
                ):
                    continue

                print(
                    "\n\nEpisode {}/{} @ Step {}".format(
                        episode, self.MAX_TRAINING_EPISODES, step
                    )
                )

                # Get action
                self.action = self.get_action_epsilon_greedy(self.state)

                # Log action
                print("action =", repr(self.action))

                # Execute action in the environment
                self.next_state, self.reward = self.env.step(self.action)

                # Log reward and next state
                print("reward =", self.reward)
                print("state =", repr(self.state))
                print("Resulting reward: ", self.reward)
                print("Resulting state: ", self.next_state)

                # Store experience
                self.replay_memory.append(
                    [self.state, self.action, self.reward, self.next_state]
                )

                # Predict Q-Value for previous state-action
                q_value = self.predict(self.state, self.action)

                best_next_state = self.max_a(self.next_state)
                # TD target (what really happened)
                td_target = self.reward + self.gamma * best_next_state

                # Calculate and print TD error
                td_error = td_target - q_value
                self.episode_mse[episode] += td_error**2

                # Log TD target, Q-value, TD error and Max_a
                print("td_target =", td_target)
                print("q_value =", q_value)
                print("td_error =", td_error)
                print("max_a =", best_next_state)
                # print("TD target:", td_target, '| Q-value', q_value, '| TD error:', td_error, "| Max_a:", best_next_state)

                with open("data/errors.dat", "a+") as f:
                    f.write(str(td_error) + "\n")

                # Update action weights
                self.update(self.state, self.action, td_target, q_value)

                # Update current state
                self.state = self.next_state

                # Perform experience replay
                if episode > 0:
                    self.experience_replay()

                # Update episode stats
                self.episode_reward[episode] += self.reward

                self.finished_episode_num = episode
                self.finished_step_num = step

                # save the agent data and env data after each step
                self.save()
                self.env.save()

                # If episode's last execution
                if step + 1 == self.MAX_STEPS_PER_EPISODE:
                    # Save weights for experience replay
                    self.frozen_action_weights = self.action_weights
                    self.frozen_feature_weights = self.feature_weights

                    # Calculate episode duration
                    self.episode_duration[episode] = time.time() - episode_start_time
                    self.episode_mse[episode] /= self.MAX_STEPS_PER_EPISODE

                    print("\n\n\n### FINISHED EPISODE %s ###" % episode)
                    print("Epsilon:", self.epsilon)
                    print("Reward:", self.episode_reward[episode])
                    print("Duration:", self.episode_duration[episode])
                    print("Feature weights:")
                    pprint.pprint(self.feature_weights)
                    print("Action weights:")
                    pprint.pprint(self.action_weights)

                    # Save data
                    self.env.post_episode(
                        episode,
                        self.episode_reward[episode],
                        self.episode_duration[episode],
                        self.episode_mse[episode],
                    )

                    # Decrease epsilon value by 20%
                    self.epsilon -= self.epsilon * 0.2

                    # Reset environment and attributes
                    self.state = self.env.reset()
                    self.next_state = None
                    self.action = None
                    self.reward = None
