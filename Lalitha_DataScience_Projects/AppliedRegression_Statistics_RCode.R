

###############################################Model2##########################
setwd("C:\\Lalitha_UHCL\\Fall_2023\\datasets")

df = read.csv("Global YouTube Statistics.csv", header = TRUE, sep = ",")
df

#check for columns names
print(names(df))


############################
##########Train and Test data split
##############################
#install.packages("caTools")
library(caTools)
# Set a random seed for reproducibility of results.
set.seed(123)

# Split the dataframe into training and testing sets using sample.split 
# to ensure that approximately 70% of the data is used for training.
splitIndex <- sample.split(df$subscribers, SplitRatio = 0.7)
train_data <- df[splitIndex, ]
test_data <- df[!splitIndex, ]

# Removed rows with any missing values (NA) in training and testing data to avoid errors in model fitting and prediction.
# Missing data can lead to 'nan' in RMSE calculations because missing values in the predictors or response 
# can't be used in model computations.
train_data <- na.omit(train_data)
test_data <- na.omit(test_data)

############################
##########Fit linear regression model and Residual Analysis
##############################
# create linear regression model
modellm <- lm(subscribers ~ video.views + uploads + video_views_for_the_last_30_days + highest_monthly_earnings + lowest_monthly_earnings +
              highest_yearly_earnings + lowest_yearly_earnings + subscribers_for_last_30_days + Gross.tertiary.education.enrollment.... + 
              Population + Unemployment.rate + Urban_population, data = train_data)

summary(modellm)
anova(modellm)
###confidence interval##########
confint(modellm, 'video.views', level=0.95)

############################
##########predictions on linear regression model
##############################
# Use the model to make predictions on the train data set to evaluate how well the model performs on unseen data (test data).
predicted_lm <- predict(modellm, test_data)

# Summarize the predictions to understand the distribution of predicted subscriber counts.
summary(predicted_lm)

# Store the actual subscriber counts from the test set to compare against the predicted values.
actual <- test_data$subscribers
summary(actual)

# Calculate the Root Mean Squared Error (RMSE) to measure the average magnitude of the errors between our predictions and actual values.
# RMSE is a standard way to measure the error of a model in predicting quantitative data.
rmse <- sqrt(mean((actual - predicted_lm)^2))

# RMSE to see how much, on average, the predictions deviate from the actual subscriber counts.
print(rmse)

# Load necessary library
library(ggplot2)

# Create a data frame for plotting
plot_rmse_data <- data.frame(Actual = actual, Predicted = predicted_lm)
plot_rmse_data

# Create the scatter plot with ggplot2
ggplot(plot_rmse_data, aes(x = Actual, y = Predicted)) +
  geom_point(color = 'blue') +  # Add points
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +  # Add a dashed line y=x
  geom_text(aes(x = quantile(Actual, 0.1), y = max(Predicted), label = paste("RMSE:", round(rmse, 2))), hjust = 0, vjust = 1, color = "red") +
  labs(x = "Actual Subscriber Counts", y = "Predicted Subscriber Counts", title = "Actual vs Predicted Subscriber Counts") +
  theme_minimal()  # Use a minimal theme

##########################
#Residual Analysis
############################

# Set up the graphics layout to a 2x2 grid
par(mfrow=c(2,2))

# Plot the four diagnostic plots
plot(modellm, which = 1) # Residuals vs. Fitted
plot(modellm, which = 2) # Normal Q-Q
plot(modellm, which = 3) # Scale-Location
plot(modellm, which = 4) # Cooks distance
plot(modellm, which = 5) # Residuals vs. Leverage

# Reset to default layout
par(mfrow=c(1,1))
############################
##########log transformation on Linear Model
##############################
#transformations on lm;
# Applied log transformation to 'subscribers'
train_data$log_subscribers = log(train_data$subscribers + 1)
train_data

# Fit the linear model with the log-transformed response variable
log_model <- lm(log_subscribers ~ video.views + uploads + video_views_for_the_last_30_days + highest_monthly_earnings + 
                  lowest_monthly_earnings + highest_yearly_earnings + lowest_yearly_earnings + subscribers_for_last_30_days + 
                  Gross.tertiary.education.enrollment.... + Population + Unemployment.rate + Urban_population, 
                data = train_data)

# Summarize the log-transformed model
summary(log_model)

# As the sum of predictors increases, the log-transformed subscribers also tend to increase.
plot(train_data$log_subscribers, train_data$video.views + train_data$uploads + train_data$video_views_for_the_last_30_days + 
       train_data$highest_monthly_earnings + train_data$lowest_monthly_earnings + train_data$highest_yearly_earnings + 
       train_data$lowest_yearly_earnings + train_data$subscribers_for_last_30_days + train_data$Gross.tertiary.education.enrollment.... + 
       train_data$Population + train_data$Unemployment.rate + train_data$Urban_population + 1, # Adding 1 to avoid log(0)
     pch=20, col=2, xlab="Sum of predictors", ylab="Log(Subscribers)")

# Set up the graphics layout to a 2x2 grid
par(mfrow=c(2,2))

# Residuals vs. Fitted Values Plot
plot(log_model, which = 1)

# Normal Q-Q Plot
plot(log_model, which = 2)

# Scale-Location (Spread-Location) Plot
plot(log_model, which = 3)

# Residuals vs. Leverage Plot
plot(log_model, which = 5)


############################
##########sqrt transformations on Linear Model
##############################

# Applied square root transformation to 'subscribers'
train_data$sqrt_subscribers = sqrt(train_data$subscribers)

# Fit the linear model with the square root transformed response variable
sqrt_model <- lm(sqrt_subscribers ~ video.views + uploads + video_views_for_the_last_30_days + highest_monthly_earnings + 
                   lowest_monthly_earnings + highest_yearly_earnings + lowest_yearly_earnings + subscribers_for_last_30_days + 
                   Gross.tertiary.education.enrollment.... + Population + Unemployment.rate + Urban_population, 
                 data = train_data)
summary(sqrt_model)
# Plot the original subscribers vs. the predictors (summed for simplicity)
plot(train_data$sqrt_subscribers, train_data$video.views + train_data$uploads + train_data$video_views_for_the_last_30_days + 
       train_data$highest_monthly_earnings + train_data$lowest_monthly_earnings + train_data$highest_yearly_earnings + 
       train_data$lowest_yearly_earnings + train_data$subscribers_for_last_30_days + train_data$Gross.tertiary.education.enrollment.... + 
       train_data$Population + train_data$Unemployment.rate + train_data$Urban_population, 
     pch=20, col=2, xlab="Sum of predictors", ylab="sqrt_subscribers")


# Set up the graphics layout to a 2x2 grid
par(mfrow=c(2,2))

# Residuals vs. Fitted Values Plot
plot(sqrt_model, which = 1)

# Normal Q-Q Plot
plot(sqrt_model, which = 2)

# Scale-Location (Spread-Location) Plot
plot(sqrt_model, which = 3)

# Residuals vs. Leverage Plot
plot(sqrt_model, which = 5)

############################
######Multicollinearity Issues
##############################
#created df with columns to check correlation
predictors_df <- train_data[, c("subscribers", "video.views", "uploads", "video_views_for_the_last_30_days", "highest_monthly_earnings", "lowest_monthly_earnings",
                                  "highest_yearly_earnings", "lowest_yearly_earnings", "subscribers_for_last_30_days", "Gross.tertiary.education.enrollment....", 
                                  "Population", "Unemployment.rate", "Urban_population")]

# correlation matrix...
cor_matrix = round(cor(predictors_df),2)
cor_matrix
# correlation matrix...plotting
pairs(cor_matrix)

############################
######Polynomial Regression Model
##############################
#########################################      
# Multiple models test to see which one is the best model
# Fit a linear model with the new polynomial and interaction terms
model_poly_interaction_1 <- lm(subscribers ~ uploads + highest_monthly_earnings +I(uploads^2) + I(highest_monthly_earnings^2) + I(uploads * highest_monthly_earnings) ,data = train_data)
# Summarize the new model
summary(model_poly_interaction_1)
anova(model_poly_interaction_1)


model_poly_interaction_2 <- lm(subscribers ~ video.views + Population +I(video.views^2) + I(Population^2) + I(video.views * Population) ,data = train_data)
# Summarize the new model
summary(model_poly_interaction_2)

model_poly_interaction_4 <- lm(subscribers ~ Urban_population + video.views +I(Urban_population^2) + I(video.views^2) + I(Urban_population * video.views) ,data = train_data)
# Summarize the new model
summary(model_poly_interaction_4)

model_poly_interaction_5 <- lm(subscribers ~ video.views + highest_monthly_earnings +I(video.views^2) + I(highest_monthly_earnings^2) + I(video.views * highest_monthly_earnings) ,data = train_data)
# Summarize the new model
summary(model_poly_interaction_5)
anova(model_poly_interaction_5)
########

model_poly_interaction_3 <- lm(subscribers ~ video.views + Unemployment.rate +I(video.views^2) + I(Unemployment.rate^2) + I(video.views * Unemployment.rate) ,data = train_data)
# Summarize the new model
summary(model_poly_interaction_3)

# Set up the graphics layout to a 2x2 grid
par(mfrow=c(2,2))

# Residuals vs. Fitted Values Plot
plot(model_poly_interaction_3, which = 1)

# Normal Q-Q Plot
plot(model_poly_interaction_3, which = 2)

# Scale-Location (Spread-Location) Plot
plot(model_poly_interaction_3, which = 3)

# Residuals vs. Leverage Plot
plot(model_poly_interaction_3, which = 5)
##########################
##########by category
##########################
# Convert Category to a factor if it's not already
df$category <- as.factor(df$category)
df$category

# Build the regression model
lmmodel_Category <- lm(subscribers ~ video.views + category, data = train_data)
summary(lmmodel_Category)

# Build the regression model with interaction terms
lmmodel_Category_interaction <- lm(subscribers ~ video.views + category + (video.views * category), data = train_data)
summary(lmmodel_Category_interaction)

##########################
##########by category and video view rank
##########################
# Convert Category to a factor if it's not already
df$category <- as.factor(df$category)
df$category

# Build the regression model with an interaction term
lmmodel_Category_viewrank <- lm(subscribers ~ video.views + category + video_views_rank + (category*video_views_rank), data = train_data)
summary(lmmodel_Category_viewrank)

##########################
#average subscribers count - by country
##########################
avg_subscribers = aggregate(subscribers ~ Country, data = train_data, FUN = mean)
avg_subscribers

country_with_lowest_avg <- avg_subscribers$Country[which.min(avg_subscribers$subscribers)]
country_with_lowest_avg
country_with_highest_avg <- avg_subscribers$Country[which.max(avg_subscribers$subscribers)]
country_with_highest_avg

##########################
#anova - by country
##########################
model_Country <- lm(subscribers ~ Country, data = train_data)
anova(model_Country)
summary(model_Country)




















































